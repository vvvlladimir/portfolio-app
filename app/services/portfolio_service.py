import pandas as pd
from typing import Iterable, Tuple, Dict, Optional
from fastapi import Depends
from app.api.dependencies import get_factory
from app.repositories import RepositoryFactory
from app.services.fx_rates_service import FXRateService


def _resolve_fx_pairs(
        df_fx: pd.DataFrame,
        base_currency: str,
        currencies: Iterable[str],
) -> Tuple[Dict[str, Optional[str]], Dict[str, bool]]:
    """
    For each currency returns:
      - fx_ticker for conversion to base_currency (or None, if base==curr)
      - flag is_inverse (whether to use 1/rate)
    """
    available = set(df_fx["fx_ticker"].unique())
    map_pair: Dict[str, Optional[str]] = {}
    map_inv: Dict[str, bool] = {}

    for curr in set(currencies):
        if curr == base_currency:
            map_pair[curr] = None
            map_inv[curr] = False
            continue

        direct = f"{curr}{base_currency}=X"
        inverse = f"{base_currency}{curr}=X"

        if direct in available:
            map_pair[curr] = direct
            map_inv[curr] = False
        elif inverse in available:
            map_pair[curr] = inverse
            map_inv[curr] = True
        else:
            # no pair — leave None and then substitute rate=1.0,
            map_pair[curr] = None
            map_inv[curr] = False

    return map_pair, map_inv


def _build_portfolio_timeseries(
        df_positions: pd.DataFrame,
        df_prices: pd.DataFrame,
        df_fx: pd.DataFrame,
        base_currency: str,
        expand_daily: bool = True,
) -> pd.DataFrame:
    """
    Returns a timeseries at (ticker, date) level with columns:
      ['ticker','date','shares','close','currency','rate','value',
       'gross_invested?','gross_withdrawn?'] (where present).
    """
    dfp = df_positions.copy()
    dpr = df_prices.copy()

    dfp["date"] = pd.to_datetime(dfp["date"])
    dpr["date"] = pd.to_datetime(dpr["date"])

    start = dfp["date"].min()
    end = dpr["date"].max()
    if expand_daily:
        all_dates = pd.date_range(start, end, freq="D")
        tickers = dfp["ticker"].unique()
        df_all = pd.MultiIndex.from_product(
            [tickers, all_dates], names=["ticker", "date"]
        ).to_frame(index=False)
    else:
        df_all = dfp[["ticker", "date"]].drop_duplicates()

    # positions + calendar
    base = (
        df_all
        .merge(dfp.drop(columns=[c for c in ("close",) if c in dfp.columns]),
               on=["ticker", "date"], how="left")
        .merge(dpr[["date", "ticker", "close"]], on=["date", "ticker"], how="left")
        .sort_values(["ticker", "date"])
    )

    # from the first purchase per ticker
    first_buy = dfp.groupby("ticker")["date"].min().rename("first_buy")
    base = base.merge(first_buy, on="ticker", how="left")
    base = base[base["date"] >= base["first_buy"]].drop(columns="first_buy")

    # forward-fill per ticker
    cols_ffill = [c for c in ["shares", "currency", "close"] if c in base.columns]
    base[cols_ffill] = base.groupby("ticker")[cols_ffill].ffill()

    # FX mapping
    fx_map, inv_map = _resolve_fx_pairs(
        df_fx=df_fx,
        base_currency=base_currency,
        currencies=base["currency"].dropna().unique()
    )
    base["fx_ticker"] = base["currency"].map(fx_map)
    base["is_inverse"] = base["currency"].map(inv_map).fillna(False)

    base = base.merge(df_fx, on=["date", "fx_ticker"], how="left")
    base["rate"] = base.groupby("ticker")["rate"].ffill()
    base["rate"] = base["rate"].fillna(1.0).astype(float)
    base.loc[base["is_inverse"], "rate"] = 1.0 / base.loc[base["is_inverse"], "rate"]

    # Recalculate value and possible cash flows to base currency
    base["value"] = base["close"] * base["shares"] * base["rate"]
    if "gross_invested" in base.columns:
        base["gross_invested"] = base["gross_invested"] * base["rate"]
    if "gross_withdrawn" in base.columns:
        base["gross_withdrawn"] = base["gross_withdrawn"] * base["rate"]

    return base[[
        c for c in [
            "date", "ticker", "shares", "close", "currency",
            "fx_ticker", "is_inverse", "rate", "value",
            "gross_invested", "gross_withdrawn"
        ] if c in base.columns
    ]]



def calculate_portfolio_history(
        df_positions: pd.DataFrame,
        df_prices: pd.DataFrame,
        base_currency: str = "USD",
        factory: RepositoryFactory = Depends(get_factory),
) -> pd.DataFrame:
    """
    Daily portfolio history: total_value, cash-flows (invest/withdraw), invested_value.
    """
    # FX only for required currencies
    df_positions = df_positions.copy()
    df_positions["date"] = pd.to_datetime(df_positions["date"])
    df_prices = df_prices.copy()
    df_prices["date"] = pd.to_datetime(df_prices["date"])

    currencies = set(df_positions["currency"].unique())
    currencies.discard(base_currency)
    df_fx = FXRateService(factory).get_fx_rates(
        currencies, df_prices, target_currency=base_currency,
        start_date=df_positions["date"].min()
    )

    base = _build_portfolio_timeseries(
        df_positions=df_positions,
        df_prices=df_prices,
        df_fx=df_fx,
        base_currency=base_currency,
        expand_daily=True
    )

    # Aggregate by date
    agg_cols = ["value"]
    if "gross_invested" in base.columns:
        agg_cols.append("gross_invested")
    if "gross_withdrawn" in base.columns:
        agg_cols.append("gross_withdrawn")

    out = (
        base.groupby("date", as_index=False)[agg_cols].sum()
        .rename(columns={"value": "total_value"})
        .sort_values("date")
    )

    if "gross_invested" in out.columns and "gross_withdrawn" in out.columns:
        out["invested_value"] = (
                out["gross_invested"].cumsum() - out["gross_withdrawn"].cumsum()
        )

    return out


def calculate_portfolio_weights(
        df_positions: pd.DataFrame,
        df_prices: pd.DataFrame,
        base_currency: str = "USD",
        factory: RepositoryFactory = Depends(get_factory),
) -> pd.DataFrame:
    """
    Weight of each position (in portfolio currency).
    """
    df_positions = df_positions.copy()
    df_positions["date"] = pd.to_datetime(df_positions["date"])
    df_prices = df_prices.copy()
    df_prices["date"] = pd.to_datetime(df_prices["date"])

    currencies = set(df_positions["currency"].unique())
    currencies.discard(base_currency)
    df_fx = FXRateService(factory).get_fx_rates(
        currencies, df_prices, target_currency=base_currency, start_date=df_positions["date"].min()
    )

    base = _build_portfolio_timeseries(
        df_positions=df_positions,
        df_prices=df_prices,
        df_fx=df_fx,
        base_currency=base_currency,
        expand_daily=False
    )

    base["total_value"] = base.groupby("date")["value"].transform("sum")
    base["weight"] = base["value"] / base["total_value"]

    # return weights on the latest date
    latest = (
        base.sort_values(["ticker", "date"])
        .groupby("ticker", as_index=False)
        .last()[["ticker", "weight"]]
    )
    return latest

