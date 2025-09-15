import pandas as pd
from sqlalchemy.orm import Session
from fastapi import Depends
from sqlalchemy import text
from app.database import PortfolioHistory, Transaction, TickerInfo, Price, Position,get_db

from sqlalchemy.orm import Session
import pandas as pd



def calculate_portfolio_history(db: Session, base_currency: str = "USD") -> pd.DataFrame:
    # --- 1. Позиции ---
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

    df_positions["market_value"] = df_positions["market_value"].astype(float)
    df_fx_rates["rate"] = df_fx_rates["rate"].astype(float)
    print(df_positions.head())
    print(df_transactions.head())
    print(df_fx_rates.head())

    def make_pair(from_cur, to_cur):
        if from_cur == to_cur:
            return None
        return f"{from_cur}{to_cur}=X"

    # добавляем колонку с нужной парой
    df_positions["pair"] = df_positions["currency"].apply(
        lambda cur: make_pair(cur, base_currency)
    )

    # сортируем для merge_asof
    df_positions = df_positions.sort_values("date")
    df_fx_rates = df_fx_rates.sort_values("date")

    # делаем merge_asof: берём последний доступный курс на дату
    df_merged = pd.merge_asof(
        df_positions,
        df_fx_rates,
        by="pair",
        on="date",
        direction="backward"  # берём ближайший прошлый курс
    )

    # ---------- Расчёт в базовой валюте ----------
    def convert_value(row):
        if row["currency"] == base_currency:
            return float(row["market_value"])
        else:
            return float(row["market_value"]) * float(row["rate"])

    df_merged["value_in_base"] = df_merged.apply(convert_value, axis=1)

    # ---------- Итоговый портфель по датам ----------
    portfolio = (
        df_merged.groupby("date")["value_in_base"]
        .sum()
        .reset_index()
    )

    print(portfolio)

calculate_portfolio_history(next(get_db()), base_currency="USD")

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