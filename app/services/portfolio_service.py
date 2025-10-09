import pandas as pd

def calculate_portfolio_history(
        df_positions: pd.DataFrame,
        df_prices: pd.DataFrame,
        base_currency: str = "USD"
) -> pd.DataFrame:

    df_positions['date'] = pd.to_datetime(df_positions['date'])
    df_prices['date'] = pd.to_datetime(df_prices['date'])

    currencies = set(df_positions["currency"].unique())
    currencies.discard(base_currency)
    fx_pairs = [f"{a}{base_currency}=X" for a in currencies] + [f"{base_currency}{a}=X" for a in currencies]
    df_fx = (
        df_prices[df_prices['ticker'].isin(fx_pairs)]
        .loc[df_prices['date'] >= df_positions['date'].min()]
        [['ticker', 'date', 'close']]
        .rename(columns={'ticker': 'fx_ticker', 'close': 'rate'})
    )

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
    # df_portfolio['total_return'] = df_portfolio['total_value'] - df_portfolio['invested_value']
    df_portfolio = df_portfolio.reset_index()
    return df_portfolio

# calculate_portfolio_history(next(get_db()), base_currency="RUB")