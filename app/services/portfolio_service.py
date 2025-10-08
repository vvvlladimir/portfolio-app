from app.database.database import Transaction, TickerInfo, Price, Position, get_db
from app.models.schemas import TransactionsOut
from app.models.transactions import TransactionType
from sqlalchemy.orm import Session, joinedload
import pandas as pd
from sqlalchemy import cast, Float

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
        db.query(
            Position.date,
            Position.ticker,
            cast(Position.shares, Float),
            cast(Position.close, Float),
            cast(Position.gross_invested, Float),
            cast(Position.gross_withdrawn, Float),
            TickerInfo.currency
        )
        .join(TickerInfo, Position.ticker == TickerInfo.ticker)
        .all()
    )

    df_positions = pd.DataFrame(positions, columns=["date", "ticker", "shares", "close", "gross_invested", "gross_withdrawn", "currency"])
    df_positions['date'] = pd.to_datetime(df_positions['date'])

    prices = (
        db.query(
            Price.date,
            Price.ticker,
            cast(Price.close, Float),
        )
        .all()
    )

    df_prices = pd.DataFrame(prices, columns=["date", "ticker", "close"])
    df_prices['date'] = pd.to_datetime(df_prices['date'])

    currencies = set(df_positions["currency"].unique())
    currencies.discard(base_currency)
    fx_pairs = [f"{a}{base_currency}=X" for a in currencies] + [f"{base_currency}{a}=X" for a in currencies]
    df_fx = (
        db.query(
            Price.ticker,
            Price.date,
            Price.close)
        .filter(Price.ticker.in_(fx_pairs))
        .filter(Price.date >= df_positions['date'].min())
        .all()
    )

    df_fx = pd.DataFrame(df_fx, columns=["fx_ticker", "date", "rate"])
    df_fx["date"] = pd.to_datetime(df_fx["date"])

    all_dates = pd.date_range(df_positions['date'].min(), df_prices['date'].max(), freq='D')

    df_portfolio = (
        df_positions
        .set_index('date')
        .groupby('ticker')[['shares','currency','gross_invested','gross_withdrawn']]
        .apply(lambda s: s.reindex(all_dates))
        .reset_index()
        .rename(columns={'level_1': 'date'})
    )
    currencies = set(df_fx["fx_ticker"].unique())

    def get_fx_ticker(curr):
        if curr == base_currency:
            return base_currency

        direct = f"{curr}{base_currency}=X"
        if direct in currencies:
            return direct

        inverse = f"{base_currency}{curr}=X"
        if inverse in currencies:
            return inverse

    unique_currencies = df_portfolio["currency"].dropna().unique()
    currency_mapping = {curr: get_fx_ticker(curr) for curr in unique_currencies}

    df_portfolio['fx_ticker'] = df_portfolio['currency'].map(currency_mapping)

    # first_buy = df_positions.groupby('ticker')['date'].min().rename('first_buy')
    # df_portfolio = df_portfolio.merge(first_buy, on='ticker', how='left')
    # df_portfolio = df_portfolio[df_portfolio['date'] >= df_portfolio['first_buy']].drop(columns='first_buy')
    #
    df_portfolio = (
        df_portfolio.drop_duplicates()
        .merge(df_prices[['date','ticker','close']], on=['date','ticker'], how='left')
        .sort_values(['ticker','date'])
    )

    df_portfolio[['shares','currency','close', 'fx_ticker']] = (
        df_portfolio.groupby('ticker')[['shares','currency','close', 'fx_ticker']].ffill()
    )
    df_portfolio = df_portfolio.merge(
        df_fx, on=["date", "fx_ticker"], how="left"
    )

    df_portfolio["rate"] = df_portfolio.groupby('ticker')["rate"].ffill().fillna(1.0).astype(float)
    df_portfolio['value'] = df_portfolio['close'] * df_portfolio['shares'] * df_portfolio['rate']
    df_portfolio['gross_invested'] = df_portfolio['gross_invested'] * df_portfolio['rate']
    df_portfolio['gross_withdrawn'] = df_portfolio['gross_withdrawn'] * df_portfolio['rate']
    df_portfolio = (
        df_portfolio.groupby('date', as_index=True)[['value', 'gross_invested','gross_withdrawn']].sum()
        .rename(columns={'value':'total_value'})
    )
    df_portfolio['invested_value'] = df_portfolio['gross_invested'].cumsum() - df_portfolio['gross_withdrawn'].cumsum()
    df_portfolio['total_return'] = df_portfolio['total_value'] - df_portfolio['invested_value']
    df_portfolio = df_portfolio.reset_index()
    print(df_portfolio['date'])
    return df_portfolio

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
            gross_withdrawn=lambda x: x["value_converted"].where(x["type"] == "SELL", 0),
        )
        .groupby(["ticker", "date"], as_index=False)
        .agg({
            "shares": "sum",
            "gross_invested": "sum",
            "gross_withdrawn": "sum",
            "close": "first"
        })
        .sort_values(["ticker", "date"])
    )

    df_positions[["shares","cum_invested", "cum_withdraw"]] = (
        df_positions.groupby("ticker")[["shares","gross_invested", "gross_withdrawn"]].cumsum()
    )
    df_positions['value'] = df_positions['shares'] * df_positions['close']
    df_positions["total_pnl"] = (
            df_positions["value"] + df_positions["cum_withdraw"] - df_positions["cum_invested"]
    )
    # df_positions["total_return_pct"] = (
    #         df_positions["total_pnl"] / df_positions["gross_invested"]
    # )
    # df_positions.set_index(["date", "ticker"], inplace=True)
    df_positions["date"] = pd.to_datetime(df_positions["date"])

    return df_positions