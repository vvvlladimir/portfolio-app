import sys
print("Python executable:", sys.executable)

from sqlalchemy import text
from app.database import PortfolioHistory, Transaction, TickerInfo, Price, Position,get_db
from app.models.schemas import PortfolioHistoryOut, TransactionsOut, PositionsOut
from app.models.transactions import TransactionType
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, joinedload
import pandas as pd
from collections import deque

class FIFOTrader:
    """Класс для учёта по FIFO"""
    def __init__(self):
        self.lots = deque()
        self.realized_pnl = 0.0

    def buy(self, qty, price):
        self.lots.append([qty, price])

    def sell(self, qty, price):
        remaining = qty
        while remaining > 0 and self.lots:
            lot_qty, lot_price = self.lots[0]
            sell_qty = min(remaining, lot_qty)
            pnl = (price - lot_price) * sell_qty
            self.realized_pnl += pnl

            lot_qty -= sell_qty
            remaining -= sell_qty
            if lot_qty == 0:
                self.lots.popleft()
            else:
                self.lots[0][0] = lot_qty

    def unrealized_pnl(self, current_price):
        return sum((current_price - lot_price) * lot_qty for lot_qty, lot_price in self.lots)

    def total_pnl(self, current_price):
        return self.realized_pnl + self.unrealized_pnl(current_price)


def add_fifo_pnl(df: pd.DataFrame,
                 date_col: str = "date",
                 ticker_col: str = "ticker",
                 shares_col: str = "shares",
                 price_col: str = "price",
                 close_col: str = "close") -> pd.DataFrame:
    """
    Добавляет FIFO PnL (realized, unrealized, total) в DataFrame.

    Аргументы:
        df : DataFrame с колонками (по умолчанию):
            - date : дата
            - ticker : идентификатор актива
            - shares : количество на дату
            - price : цена сделки (если была покупка/продажа)
            - close : рыночная цена
        *_col : названия колонок (если в df другие имена)

    Возвращает:
        DataFrame с новыми колонками:
            - realized_pnl
            - unrealized_pnl
            - total_pnl
    """

    df = df.copy().reset_index()
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values([ticker_col, date_col])

    traders = {}
    realized_list, unrealized_list, total_list = [], [], []

    for _, row in df.iterrows():
        date = row[date_col]
        ticker = row[ticker_col]
        shares = row[shares_col]
        close = row[close_col]
        price = row[price_col]

        if ticker not in traders:
            traders[ticker] = FIFOTrader()
            prev_shares = 0
        else:
            prev_idx = (df.index < row.name) & (df[ticker_col] == ticker)
            prev_shares = df.loc[prev_idx, shares_col].iloc[-1] if prev_idx.any() else 0

        trader = traders[ticker]
        delta = shares - prev_shares

        if delta > 0:
            trader.buy(delta, price)
        elif delta < 0:
            trader.sell(-delta, price)

        realized = trader.realized_pnl
        unrealized = trader.unrealized_pnl(close)
        total = realized + unrealized

        realized_list.append(realized)
        unrealized_list.append(unrealized)
        total_list.append(total)

    df["realized_pnl"] = realized_list
    df["unrealized_pnl"] = unrealized_list
    df["total_pnl"] = total_list
    df["total_pnl_pct"] = total_list / (df[shares_col] * df[price_col]).replace(0, pd.NA)

    return df.sort_values(["date", "ticker"]).set_index(["date", "ticker"])

def get_transactions(db: Session):
    try:
        rows = (db.query(Transaction)
                    .options(joinedload(Transaction.ticker_info))
                    .all())
        return [TransactionsOut.model_validate(row) for row in rows]
    except Exception as e:
        db.rollback()
        return {"status": "error", "message": str(e)}

def expand_with_full_calendar(df, value_cols, start=None, end=None):
    if not isinstance(df.index, pd.MultiIndex):
        df = df.set_index(["date", "ticker"])

    if start is None:
        start = df.index.get_level_values("date").min()
    if end is None:
        end = pd.Timestamp.today().normalize()

    full_range = pd.date_range(start, end, freq="D")
    tickers = df.index.get_level_values("ticker").unique()
    full_index = pd.MultiIndex.from_product([full_range, tickers], names=["date", "ticker"])

    df_expanded = df.reindex(full_index)
    df_expanded[value_cols] = (
        df_expanded.groupby("ticker")[value_cols].ffill().fillna(0)
    )
    return df_expanded.reset_index()

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
        db.query(Position.date, Position.ticker, Position.value, TickerInfo.currency)
        .join(TickerInfo, Position.ticker == TickerInfo.ticker)
        .all()
    )
    df_positions = pd.DataFrame(positions, columns=["date", "ticker", "value", "currency"])

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
        lambda row: convert_value(row, base_currency, df_fx_rates, "value"),
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

def upsert_portfolio_history(db: Session, base_currency: str = "USD") -> int:
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

        # достаём все новые строки
        rows = db.query(PortfolioHistory).all()

        return [PortfolioHistoryOut.model_validate(row) for row in rows]
    except SQLAlchemyError as e:
        db.rollback()
        print("Error upserting portfolio history:", str(e))
        raise

# calculate_portfolio_history(next(get_db()), base_currency="RUB")

def calculate_positions(db: Session):

    transactions = db.query(
        Transaction.date,
        Transaction.type,
        Transaction.ticker,
        Transaction.shares,
        Transaction.value,
        Transaction.currency
    ).all()

    prices = (
        db.query(
            Price.ticker,
            Price.date,
            Price.open,
            Price.high,
            Price.low,
            Price.close,
            Price.volume,
            TickerInfo.currency,
            TickerInfo.asset_type
        )
        .join(TickerInfo, Price.ticker == TickerInfo.ticker)
        .all()
    )

    df_prices = pd.DataFrame(prices, columns=["ticker", "date", "open", "high", "low", "close", "volume", "currency", "asset_type"])
    df_transactions = pd.DataFrame(transactions, columns=["date", "type","ticker", "shares", "value", "currency"])

    df_transactions["value"] = pd.to_numeric(df_transactions["value"], errors="coerce")
    df_transactions["shares"] = pd.to_numeric(df_transactions["shares"], errors="coerce")
    df_prices["close"] = pd.to_numeric(df_prices["close"], errors="coerce")
    # df_transactions["date"] = pd.to_datetime(df_transactions["date"])
    # df_prices["date"] = pd.to_datetime(df_prices["date"])

    df_positions = df_transactions.assign(
        shares_change=df_transactions.apply(
            lambda x: x["shares"] if x["type"] == "BUY" else -x["shares"], axis=1
        )
    )
    df_positions = (
        df_positions.groupby(["ticker", "date"])["shares_change"]
        .sum()
        .unstack(fill_value=0)
        .cumsum(axis=1)
        .stack()
        .reset_index(name="shares")
    )
    df_positions = df_positions.set_index(["date", "ticker"])
    df_positions = expand_with_full_calendar(df_positions, ["shares"])

    ticker_currency = df_prices.groupby("ticker")["currency"].first()

    df_transactions = df_transactions.assign(
        market_currency=df_transactions["ticker"].map(ticker_currency)
    )

    fx_rates = (
        df_prices[df_prices["asset_type"] == "CURRENCY"]
        .rename(columns={"ticker": "pair", "close": "rate"})
        .loc[:, ["date", "pair", "rate"]]
        .dropna()
    )

    def convert_value(row):
        if row["currency"] == row["market_currency"]:
            return row["value"]
        rate = get_rate(row["currency"], row["market_currency"], row["date"], fx_rates)
        return row["value"] * rate

    df_transactions["value_converted"] = df_transactions.apply(convert_value, axis=1)
    df_transactions["price"] = (
            df_transactions["value_converted"] / df_transactions["shares"]
    ).round(8)
    df_transaction_price = df_transactions[["date", "ticker", "price"]]

    df_prices_all = (
        pd.merge(
            df_prices[["date", "ticker", "close"]],
            df_transaction_price,
            on=["date", "ticker"],
            how="left"
        )
        .groupby(["date", "ticker"], as_index=False).first()
    )
    df_prices_all = expand_with_full_calendar(df_prices_all, ["close"])
    df_prices_all["price"] = df_prices_all["price"].fillna(df_prices_all["close"])
    df_prices_all["date"] = pd.to_datetime(df_prices_all["date"])


    df_positions = (
        df_positions
        .merge(df_prices_all, on=["date", "ticker"], how="left")
        .set_index(["date", "ticker"])
    )
    df_positions["position_value"] = df_positions["shares"] * df_positions["price"]
    df_positions["market_daily_return_pct"] = (
        df_positions.groupby("ticker")["price"].pct_change()
    )

    return add_fifo_pnl(df_positions)

def upsert_positions(db: Session) -> int:
    try:
        data = calculate_positions(db)

        db.query(Position).delete()
        rows_to_insert = []
        for _, row in data.iterrows():
            rows_to_insert.append({
                "date": row["date"].date(),
                "ticker": row["ticker"],
                "shares": float(row["shares"]) if not pd.isna(row["shares"]) else None,
                "price": float(row["price"]) if not pd.isna(row["price"]) else None,
                "position_value": float(row["position_value"]) if not pd.isna(row["position_value"]) else None,
                "market_daily_return_pct": float(row["market_daily_return_pct"]) if not pd.isna(row["market_daily_return_pct"]) else None,
                "total_pnl": float(row["total_pnl"]) if not pd.isna(row["total_pnl"]) else None,
            })

        # bulk insert
        stmt = insert(Position)
        db.execute(stmt, rows_to_insert)
        db.commit()

        rows = db.query(Position).all()
        return [PositionsOut.model_validate(row) for row in rows]
    except SQLAlchemyError as e:
        db.rollback()
        print("Error upserting portfolio history:", str(e))
        raise

print(upsert_positions(next(get_db())))