import pandas as pd
from fastapi import Depends

from app.api.dependencies import get_factory
from app.repositories import RepositoryFactory
from app.services.fx_rates_service import FXRateService


def calculate_positions(
        df_transactions: pd.DataFrame,
        df_prices: pd.DataFrame,
        factory: RepositoryFactory = Depends(get_factory),
) -> pd.DataFrame:
    """Calculate positions over time from transactions and prices."""

    ticker_currency = df_prices.groupby("ticker")["currency"].first()
    df_positions = df_transactions.copy()
    df_positions = df_positions.assign(
        market_currency=df_positions["ticker"].map(ticker_currency)
    )
    df_positions["date"] = pd.to_datetime(df_positions["date"])
    currencies = set(df_positions["currency"].unique()) | set(df_positions["market_currency"].unique())
    
    df_fx = FXRateService(factory).get_fx_rates(currencies, df_prices, start_date=df_positions['date'].min())
    
    df_positions['fx_ticker'] = df_positions['currency'] + df_positions['market_currency'] + "=X"
    df_positions = pd.merge_asof(
        df_positions,
        df_fx,
        on='date',
        by='fx_ticker',
        direction='nearest'
    ).rename(columns={'rate': 'rate_direct'})
    df_positions['fx_ticker'] = df_positions['market_currency'] + df_positions['currency'] + "=X"
    df_positions = pd.merge_asof(
        df_positions,
        df_fx,
        on='date',
        by='fx_ticker',
        direction='nearest'
    ).rename(columns={'rate': 'rate_inverse'})
    
    df_positions['rate'] = (df_positions['rate_direct'].fillna(1 / df_positions['rate_inverse'])).fillna(1.0)
    df_positions['value'] = df_positions['value'] * df_positions['rate']

    df_positions = df_positions[["date", "ticker", "value", "type", "shares"]]
    df_positions = df_positions.merge(df_prices[['date','ticker','close']], on=["date", "ticker"], how="left")

    df_positions = (
        df_positions.assign(
            shares=lambda x: x["shares"].where(x["type"] == "BUY", -x["shares"]),
            gross_invested=lambda x: x["value"].where(x["type"] == "BUY", 0),
            gross_withdrawn=lambda x: x["value"].where(x["type"] == "SELL", 0),
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
    df_positions["date"] = pd.to_datetime(df_positions["date"])

    return df_positions

def get_snapshot_positions(
        df_positions: pd.DataFrame,
        df_prices: pd.DataFrame,
        asof_date: pd.Timestamp = pd.Timestamp.today(),
) -> pd.DataFrame:

    """Return a snapshot of current positions with market values."""
    if asof_date is None:
        asof_date = df_prices["date"].max()
    asof_date = pd.Timestamp(asof_date)

    df_positions["date"] = pd.to_datetime(df_positions["date"], errors="coerce")
    df_positions = df_positions[df_positions["date"] <= asof_date]

    df_positions["cum_invested"] = df_positions.groupby("ticker")["gross_invested"].cumsum()
    df_positions["cum_withdrawn"] = df_positions.groupby("ticker")["gross_withdrawn"].cumsum()

    df_positions['date'] = asof_date
    df_positions = df_positions.rename(columns={"date": "asof_date"})
    df_positions = df_positions.groupby("ticker", as_index=False).tail(1).sort_values(["asof_date","ticker"])

    df_positions["asof_date"] = pd.to_datetime(df_positions["asof_date"]).astype("datetime64[ns]")
    df_prices["date"] = pd.to_datetime(df_prices["date"]).astype("datetime64[ns]")

    df_positions = pd.merge_asof(
        df_positions.drop(columns=["close"]),
        df_prices.sort_values(["date", "ticker"]),
        left_on="asof_date",
        right_on="date",
        by="ticker",
        direction="backward",
    )

    df_positions["value"] = df_positions["shares"] * df_positions["close"]
    df_positions["total_pnl"] = (
            df_positions["value"] + df_positions["cum_withdrawn"] - df_positions["cum_invested"]
    )
    return df_positions