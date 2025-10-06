from app.database.database import Transaction, TickerInfo, Price, Position, get_db
from app.models.schemas import TransactionsOut
from app.models.transactions import TransactionType
from sqlalchemy.orm import Session, joinedload
import pandas as pd



def get_transactions(db: Session):
    try:
        rows = (db.query(Transaction)
                    .options(joinedload(Transaction.ticker_info))
                    .all())
        return [TransactionsOut.model_validate(row) for row in rows]
    except Exception as e:
        return {"status": "error", "message": str(e)}


def get_rate(from_cur, to_cur, date, fx_rates):
    if from_cur == to_cur:
        return 1.0

    pair_direct = f"{from_cur}{to_cur}=X"
    pair_inverse = f"{to_cur}{from_cur}=X"

    candidates = fx_rates[
        (fx_rates["pair"].isin([pair_direct, pair_inverse]))
        & (fx_rates["date"] <= date)
        ].sort_values("date")

    if candidates.empty:
        raise ValueError(f"No FX-rate {from_cur}->{to_cur} at {date}")
    last_row = candidates.iloc[-1]

    if last_row["pair"] == pair_direct:
        return float(last_row["rate"])

    return 1.0 / float(last_row["rate"])

def calculate_portfolio_history(db: Session, base_currency: str = "USD") -> pd.DataFrame:
    positions = (
        db.query(Position.date, Position.ticker, Position.position_value, TickerInfo.currency)
        .join(TickerInfo, Position.ticker == TickerInfo.ticker)
        .all()
    )
    df_positions = pd.DataFrame(positions, columns=["date", "ticker", "value", "currency"])

    transactions = (
        db.query(Transaction.date, Transaction.type, Transaction.value, Transaction.currency)
        .all()
    )
    df_transactions = pd.DataFrame(transactions, columns=["date", "type", "value", "currency"])

    if df_positions.empty and df_transactions.empty:
        return pd.DataFrame(columns=["date", "total_value", "invested_value", "pnl"])

    currencies = set(df_positions["currency"].unique()) | set(df_transactions["currency"].unique())
    currencies.discard(base_currency)

    fx_pairs = [f"{a}{base_currency}=X" for a in currencies] + [f"{base_currency}{a}=X" for a in currencies]
    fx = (
        db.query(Price.ticker, Price.date, Price.close)
        .filter(Price.ticker.in_(fx_pairs))
        .all()
    )
    df_fx_rates = pd.DataFrame(fx, columns=["pair", "date", "rate"])
    df_fx_rates["date"] = pd.to_datetime(df_fx_rates["date"])

    df_positions["date"] = pd.to_datetime(df_positions["date"])
    df_transactions["date"] = pd.to_datetime(df_transactions["date"])

    def convert_value(row, value_col="value"):
        rate = get_rate(row["currency"], base_currency, row["date"], df_fx_rates)
        return float(row[value_col]) * rate

    df_positions["total_value"] = df_positions.apply(convert_value, axis=1)
    df_transactions["amount_in_base"] = df_transactions.apply(convert_value, axis=1)

    portfolio = (
        df_positions.groupby("date")["total_value"]
        .sum()
        .reset_index()
    )

    df_cashflows = (
        df_transactions.groupby("date")
        .apply(lambda df: pd.Series({
            "gross_invested": df.loc[df["type"].isin(TransactionType.inflows()), "amount_in_base"].sum(),
            "gross_withdrawn": df.loc[df["type"].isin(TransactionType.outflows()), "amount_in_base"].sum()
        }))
        .reset_index()
    )

    report = pd.merge(portfolio, df_cashflows, on="date", how="outer").fillna(0)
    report["invested_value"] = report["gross_invested"].cumsum() - report["gross_withdrawn"].cumsum()

    return report

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
    df_transactions = df_transactions[["date", "ticker", "value_converted", "type", "shares"]]
    df_transactions = (
        df_transactions
        .merge(df_prices[['date','ticker','close']], on=["date", "ticker"], how="left")
    )

    df_positions = (
        df_transactions.assign(
            shares=lambda x: x["shares"].where(x["type"] == "BUY", -x["shares"]),
            gross_invested=lambda x: x["value_converted"].where(x["type"] == "BUY", 0),
            gross_withdraw=lambda x: x["value_converted"].where(x["type"] == "SELL", 0),
        )
        .groupby(["ticker", "date"], as_index=False)
        .agg({
            "shares": "sum",
            "gross_invested": "sum",
            "gross_withdraw": "sum",
            "close": "first"
        })
        .sort_values(["ticker", "date"])
    )

    df_positions[["shares","gross_invested", "gross_withdraw"]] = (
        df_positions.groupby("ticker")[["shares","gross_invested", "gross_withdraw"]].cumsum()
    )
    df_positions['value'] = df_positions['shares'] * df_positions['close']
    df_positions["total_pnl"] = (
            df_positions["value"] + df_positions["gross_withdraw"] - df_positions["gross_invested"]
    )
    # df_positions["total_return_pct"] = (
    #         df_positions["total_pnl"] / df_positions["gross_invested"]
    # )
    # df_positions.set_index(["date", "ticker"], inplace=True)
    df_positions["date"] = pd.to_datetime(df_positions["date"])

    return df_positions