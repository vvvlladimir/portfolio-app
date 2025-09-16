import pandas as pd
from sqlalchemy.orm import Session
from fastapi import Depends
from sqlalchemy import text
from app.database import PortfolioHistory, Transaction, TickerInfo, Price, Position,get_db
from app.models.transactions import TransactionType
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
import pandas as pd

def get_rate(from_cur, to_cur, date, fx_rates, max_lag_days=30):
    """
    Возвращает курс from_cur -> to_cur на дату или ближайший прошлый.
    Приоритет: прямая пара, затем обратная.
    max_lag_days ограничивает, насколько далеко в прошлое можно уйти.
    """

    if from_cur == to_cur:
        return 1.0

    # строим пары
    pair_direct = f"{from_cur}{to_cur}=X"
    pair_inverse = f"{to_cur}{from_cur}=X"

    # выбираем обе пары сразу
    candidates = fx_rates[
        (fx_rates["pair"].isin([pair_direct, pair_inverse]))
        & (fx_rates["date"] <= date)
        ].sort_values("date")

    if candidates.empty:
        raise ValueError(f"Нет курса для {from_cur}->{to_cur} на {date}")

    # берём последнюю доступную запись
    last_row = candidates.iloc[-1]

    # проверяем давность курса
    if (date - last_row["date"]).days > max_lag_days:
        raise ValueError(
            f"Нет свежего курса для {from_cur}->{to_cur} на {date}, "
            f"последний: {last_row['date'].date()}"
        )

    # если это прямая пара → используем rate
    if last_row["pair"] == pair_direct:
        return float(last_row["rate"])

    # иначе обратная → инверсия
    return 1.0 / float(last_row["rate"])

def calculate_portfolio_history(db: Session, base_currency: str = "USD") -> pd.DataFrame:
    positions = (
        db.query(Position.date, Position.ticker, Position.market_value, TickerInfo.currency)
        .join(TickerInfo, Position.ticker == TickerInfo.ticker)
        .all()
    )
    df_positions = pd.DataFrame(positions, columns=["date", "ticker", "market_value", "currency"])

    # --- 2. Транзакции ---
    transactions = (
        db.query(Transaction.date, Transaction.type, Transaction.shares, Transaction.value, Transaction.currency)
        .all()
    )
    df_transactions = pd.DataFrame(transactions, columns=["date", "type", "shares", "value", "currency"])

    if df_positions.empty and df_transactions.empty:
        return pd.DataFrame()

    # --- 3. Курсы валют ---
    currencies = set(df_positions["currency"].unique()) | set(df_transactions["currency"].unique())
    currencies.discard(base_currency)

    fx_pairs = [f"{a}{base_currency}=X" for a in currencies] + [f"{base_currency}{a}=X" for a in currencies]
    fx = (
        db.query(Price.ticker, Price.date, Price.close)
        .filter(Price.ticker.in_(fx_pairs))
        .all()
    )
    df_fx_rates = pd.DataFrame(fx, columns=["pair", "date", "rate"])

    df_positions["date"] = pd.to_datetime(df_positions["date"])
    df_transactions["date"] = pd.to_datetime(df_transactions["date"])
    df_fx_rates["date"] = pd.to_datetime(df_fx_rates["date"])

    def convert_value(row, base_currency, fx_rates, value_row = "value"):
        rate = get_rate(row["currency"], base_currency, row["date"], fx_rates)
        return float(row[value_row]) * rate


    df_positions["total_value"] = df_positions.apply(
        lambda row: convert_value(row, base_currency, df_fx_rates, "market_value"),
        axis=1
    )

    portfolio = (
        df_positions.groupby("date")["total_value"]
        .sum()
        .reset_index()
    )

    df_transactions["amount_in_base"] = df_transactions.apply(
        lambda row: convert_value(row, base_currency, df_fx_rates, "value"),
        axis=1
    )

    df_cashflows = (
        df_transactions.groupby("date")
        .apply(lambda df: pd.Series({
            "gross_invested": df.loc[df["type"].isin(TransactionType.inflows()), "amount_in_base"].sum(),
            "gross_withdrawn": df.loc[df["type"].isin(TransactionType.outflows()), "amount_in_base"].sum()
        }))
        .reset_index()
    )


    # ---------- Объединение ----------
    report = pd.merge(
        portfolio,
        df_cashflows,
        on="date",
        how="outer"
    ).fillna(0)
    report["invested_value"] = report["gross_invested"].cumsum() - report["gross_withdrawn"].cumsum()

    return report

def upsert_portfolio_history(db: Session, base_currency: str = "EUR") -> int:
    """
    Пересчитывает и сохраняет историю портфеля в базу.
    """

    try:
        inserted = 0
        data = calculate_portfolio_history(db, base_currency)

        db.query(PortfolioHistory).delete()
        for _, row in data.iterrows():
            stmt = insert(PortfolioHistory).values(
                date=row["date"].date(),
                total_value=float(row["total_value"]) if not pd.isna(row["total_value"]) else None,
                invested_value=float(row["invested_value"]) if not pd.isna(row["invested_value"]) else None,
                gross_invested=float(row["gross_invested"]) if not pd.isna(row["gross_invested"]) else None,
                gross_withdrawn=float(row["gross_withdrawn"]) if not pd.isna(row["gross_withdrawn"]) else None
            )
            db.execute(stmt)
            inserted += 1

        db.commit()
        return inserted
    except SQLAlchemyError as e:
        db.rollback()
        print("Error upserting portfolio history:", str(e))
        raise

# calculate_portfolio_history(next(get_db()), base_currency="RUB")

def calculate_positions(db: Session):
    """
    Пересчитывает cumulative позиции (shares) по всем тикерам и датам.
    """
    sql = text("""
               WITH
                   tx_daily AS (
                       SELECT
                           UPPER(t.ticker) AS ticker,
                           t.date,
                           SUM(CASE
                                   WHEN t.type = 'BUY'  THEN t.shares
                                   WHEN t.type = 'SELL' THEN -t.shares
                                   ELSE 0
                               END) AS signed_shares
                       FROM transactions t
                       GROUP BY 1,2
                   ),
                   calendar AS (
                       SELECT gs::date AS date
               FROM generate_series(
                   (SELECT MIN(date) FROM transactions),
                   (SELECT MAX(date) FROM prices),
                   interval '1 day'
                   ) gs
                   ),
                   grid AS (
               SELECT c.date, tk.ticker
               FROM calendar c
                   CROSS JOIN (SELECT DISTINCT UPPER(ticker) AS ticker FROM transactions) tk
                   ),
                   pos AS (
               SELECT
                   g.date,
                   g.ticker,
                   SUM(COALESCE(tx.signed_shares,0))
                   OVER (PARTITION BY g.ticker ORDER BY g.date) AS cum_shares
               FROM grid g
                   LEFT JOIN tx_daily tx
               ON tx.ticker = g.ticker AND tx.date = g.date
                   ),
                   px_ff AS (
               SELECT
                   g.date,
                   g.ticker,
                   (
                   SELECT p.close
                   FROM prices p
                   WHERE p.ticker = g.ticker AND p.date <= g.date
                   ORDER BY p.date DESC
                   LIMIT 1
                   ) AS close_ffill
               FROM grid g
                   )
               INSERT INTO positions (ticker, date, shares, market_value)
               SELECT
                   p.ticker,
                   p.date,
                   p.cum_shares,
                   p.cum_shares * px.close_ffill AS market_value
               FROM pos p
                        JOIN px_ff px ON px.date = p.date AND px.ticker = p.ticker
                   ON CONFLICT (ticker, date) DO UPDATE
                                                     SET shares = EXCLUDED.shares,
                                                     market_value = EXCLUDED.market_value;
               """)

    db.execute(sql)
    db.commit()