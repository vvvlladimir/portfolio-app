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