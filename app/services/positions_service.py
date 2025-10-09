import pandas as pd

def calculate_positions(
        df_transactions: pd.DataFrame,
        df_prices: pd.DataFrame
) -> pd.DataFrame:


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
    df_positions["date"] = pd.to_datetime(df_positions["date"])

    return df_positions