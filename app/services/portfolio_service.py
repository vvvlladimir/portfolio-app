import pandas as pd
from fastapi import Depends
from app.api.dependencies import get_factory
from app.repositories import RepositoryFactory
from app.services.fx_rates_service import FXRateService


def calculate_portfolio_history(
        df_positions: pd.DataFrame,
        df_prices: pd.DataFrame,
        base_currency: str = "USD",
        factory: RepositoryFactory = Depends(get_factory),
) -> pd.DataFrame:

    df_positions['date'] = pd.to_datetime(df_positions['date'])
    df_prices['date'] = pd.to_datetime(df_prices['date'])
    currencies = set(df_positions["currency"].unique())
    currencies.discard(base_currency)

    df_fx = FXRateService(factory).get_fx_rates(currencies, df_prices, target_currency=base_currency, start_date=df_positions['date'].min())
    all_dates = pd.date_range(df_positions['date'].min(), df_prices['date'].max(), freq='D')

    tickers = df_positions['ticker'].unique()
    df_all = pd.MultiIndex.from_product([tickers, all_dates], names=['ticker', 'date']).to_frame(index=False)

    df_portfolio = (
        df_all
        .merge(df_positions.drop(columns=['close']), on=['ticker', 'date'], how='left')
        .merge(df_prices[['date', 'ticker']], on=['date', 'ticker'], how='left')
        .sort_values(['ticker', 'date'])
    )
    currencies = set(df_fx["fx_ticker"].unique())

    def get_fx_ticker(curr):
        if curr == base_currency:
            return base_currency, False

        direct = f"{curr}{base_currency}=X"
        if direct in currencies:
            return direct, False

        inverse = f"{base_currency}{curr}=X"
        if inverse in currencies:
            return inverse, True

        return None, False

    currency_mapping = {}
    inverse_flags = {}

    for curr in df_portfolio["currency"].dropna().unique():
        fx_ticker, is_inverse = get_fx_ticker(curr)
        currency_mapping[curr] = fx_ticker
        inverse_flags[curr] = is_inverse

    df_portfolio['fx_ticker'] = df_portfolio['currency'].map(currency_mapping)
    df_portfolio['is_inverse'] = df_portfolio['currency'].map(inverse_flags)
    first_buy = df_positions.groupby('ticker')['date'].min().rename('first_buy')
    df_portfolio = df_portfolio.merge(first_buy, on='ticker', how='left')
    df_portfolio = df_portfolio[df_portfolio['date'] >= df_portfolio['first_buy']].drop(columns='first_buy')

    df_portfolio = (
        df_portfolio.drop_duplicates()
        .merge(df_prices[['date','ticker','close']], on=['date','ticker'], how='left')
        .sort_values(['ticker','date'])
    )

    cols_ffill = ['shares','currency','close', 'fx_ticker','is_inverse']
    df_portfolio[cols_ffill] = df_portfolio.groupby('ticker')[cols_ffill].ffill()

    df_portfolio = df_portfolio.merge(
        df_fx, on=["date", "fx_ticker"], how="left"
    )

    df_portfolio["rate"] = df_portfolio.groupby('ticker')["rate"].ffill().fillna(1.0).astype(float)
    df_portfolio.loc[df_portfolio['is_inverse'], 'rate'] = 1 / df_portfolio.loc[df_portfolio['is_inverse'], 'rate']
    df_portfolio['value'] = df_portfolio['close'] * df_portfolio['shares'] * df_portfolio['rate']
    df_portfolio['gross_invested'] = df_portfolio['gross_invested'] * df_portfolio['rate']
    df_portfolio['gross_withdrawn'] = df_portfolio['gross_withdrawn'] * df_portfolio['rate']
    df_portfolio = (
        df_portfolio.groupby('date', as_index=True)[['value', 'gross_invested','gross_withdrawn']].sum()
        .rename(columns={'value':'total_value'})
    )
    df_portfolio['invested_value'] = df_portfolio['gross_invested'].cumsum() - df_portfolio['gross_withdrawn'].cumsum()
    df_portfolio = df_portfolio.reset_index()
    return df_portfolio