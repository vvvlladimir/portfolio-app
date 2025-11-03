from datetime import timedelta, date
from typing import Dict, List, Optional
import pandas as pd
from fastapi import Depends
from app.api.dependencies import get_factory
from app.repositories import RepositoryFactory
from app.services.fx_rates_service import FXRateService




def _build_portfolio_timeseries(
        df_positions: pd.DataFrame,
        df_prices: pd.DataFrame,
        expand_daily: bool = True,
        end_date: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    """
    Строим календарь ticker × date и к нему подтягиваем позиции и цены.
    ВАЖНО: строим только до end_date (или до макс даты из позиций/цен).
    """
    positions = df_positions.copy()
    prices = df_prices.copy()

    positions["date"] = pd.to_datetime(positions["date"])
    prices["date"] = pd.to_datetime(prices["date"])

    start = positions["date"].min()

    if end_date is None:
        end = max(positions["date"].max(), prices["date"].max())
    else:
        end = pd.Timestamp(end_date)

    if expand_daily:
        all_dates = pd.date_range(start, end, freq="D")
        tickers = positions["ticker"].unique()
        df_all = (
            pd.MultiIndex.from_product([tickers, all_dates], names=["ticker", "date"])
            .to_frame(index=False)
        )
    else:
        df_all = positions[["ticker", "date"]].drop_duplicates()

    merge_cols = ["date", "ticker", "close"]
    if "currency" in prices.columns:
        merge_cols.append("currency")

    base = (
        df_all
        .merge(
            positions.drop(columns=[c for c in ("close",) if c in positions.columns]),
            on=["ticker", "date"],
            how="left",
        )
        .merge(
            prices[merge_cols],
            on=["date", "ticker"],
            how="left",
        )
        .sort_values(["ticker", "date"])
    )

    first_buy = positions.groupby("ticker")["date"].min().rename("first_buy")
    base = base.merge(first_buy, on="ticker", how="left")
    base = base[base["date"] >= base["first_buy"]].drop(columns="first_buy")

    cols_ffill = [c for c in ["shares", "currency", "close"] if c in base.columns]
    base[cols_ffill] = base.groupby("ticker")[cols_ffill].ffill()

    cols_0 = [c for c in ["gross_invested", "gross_withdrawn"] if c in base.columns]
    base[cols_0] = base[cols_0].fillna(0)

    return base



def calculate_positions(
        df_transactions: pd.DataFrame,
        df_prices: pd.DataFrame,
        factory: RepositoryFactory = Depends(get_factory),
) -> pd.DataFrame:
    """
    Считаем позиции по дням из транзакций и цен.
    Здесь же приводим всё к валюте тикера через FX.
    """
    # узнаём рыночную валюту тикера
    ticker_currency = df_prices.groupby("ticker")["currency"].first()

    tx = df_transactions.copy()
    tx["date"] = pd.to_datetime(tx["date"])

    tx = tx.assign(
        market_currency=tx["ticker"].map(ticker_currency)
    )

    # валюты, которые нам вообще нужны
    currencies = set(tx["currency"].dropna().unique()) | set(tx["market_currency"].dropna().unique())

    # тянем курсы
    fx_service = FXRateService(factory)
    df_fx = fx_service.get_fx_rates(
        currencies,
        df_prices,
        start_date=tx["date"].min(),
    )

    # конвертируем value -> валюта рынка
    # 1) пробуем прямую пару
    tx["fx_ticker"] = tx["currency"] + tx["market_currency"] + "=X"
    tx = pd.merge_asof(
        tx.sort_values("date"),
        df_fx,
        on="date",
        by="fx_ticker",
        direction="nearest",
    ).rename(columns={"rate": "rate_direct"})

    # 2) пробуем обратную пару
    tx["fx_ticker"] = tx["market_currency"] + tx["currency"] + "=X"
    tx = pd.merge_asof(
        tx.sort_values("date"),
        df_fx,
        on="date",
        by="fx_ticker",
        direction="nearest",
    ).rename(columns={"rate": "rate_inverse"})

    tx["rate"] = (tx["rate_direct"].fillna(1 / tx["rate_inverse"])).fillna(1.0)
    tx["value"] = tx["value"] * tx["rate"]

    # оставляем базовые колонки
    tx = tx[["date", "ticker", "value", "type", "shares"]]

    # подтягиваем цену на дату сделки — для проверки и чтобы можно было потом восстановить
    tx = tx.merge(
        df_prices[["date", "ticker", "close"]],
        on=["date", "ticker"],
        how="left",
    )

    # приводим BUY/SELL к знаку
    tx = (
        tx.assign(
            shares=lambda x: x["shares"].where(x["type"] == "BUY", -x["shares"]),
            gross_invested=lambda x: x["value"].where(x["type"] == "BUY", 0.0),
            gross_withdrawn=lambda x: x["value"].where(x["type"] == "SELL", 0.0),
        )
        .groupby(["ticker", "date"], as_index=False)
        .agg(
            {
                "shares": "sum",
                "gross_invested": "sum",
                "gross_withdrawn": "sum",
                "close": "first",
            }
        )
        .sort_values(["ticker", "date"])
    )

    # накопительные итоги
    tx[["shares", "cum_invested", "cum_withdrawn"]] = (
        tx.groupby("ticker")[["shares", "gross_invested", "gross_withdrawn"]].cumsum()
    )

    # рыночная стоимость
    tx["value"] = tx["shares"] * tx["close"]

    # PnL: MV + withdrawals - invested
    tx["total_pnl"] = (
            tx["value"] + tx["cum_withdrawn"] - tx["cum_invested"]
    )

    tx["date"] = pd.to_datetime(tx["date"])

    # дневной денежный поток (понадобится для TWR)
    tx["cashflow"] = tx["gross_invested"] - tx["gross_withdrawn"]

    return tx


def get_snapshot_positions(
        df_positions: pd.DataFrame,
        df_prices: pd.DataFrame,
        date_to: pd.Timestamp = date.today(),
        expand_daily: bool = True,
        get_last: bool = False,
) -> pd.DataFrame:
    """
    Возвращает позиции по дням (или только последнюю строку по тикеру),
    гарантируя, что строим только до date_to.
    """
    date_to = pd.Timestamp(date_to)

    df_ts = _build_portfolio_timeseries(
        df_positions,
        df_prices,
        expand_daily=expand_daily or get_last,
        end_date=date_to,
    )

    # накопительные итоги
    df_ts["cum_invested"] = df_ts.groupby("ticker")["gross_invested"].cumsum()
    df_ts["cum_withdrawn"] = df_ts.groupby("ticker")["gross_withdrawn"].cumsum()

    # рыночная стоимость (цена может ffill’нуться)
    df_ts["value"] = df_ts["shares"] * df_ts["close"]

    # pnl
    df_ts["total_pnl"] = (
            df_ts["value"] + df_ts["cum_withdrawn"] - df_ts["cum_invested"]
    )

    # поток за день
    df_ts["cashflow"] = df_ts["gross_invested"] - df_ts["gross_withdrawn"]

    if get_last:
        return df_ts.groupby("ticker").last().reset_index()

    return df_ts


# ---------- 4. Хелпер для TWR по одному тикеру ----------

def compute_twr_for_window(
        df_one_ticker: pd.DataFrame,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
) -> Optional[float]:
    """
    Честный TWR: выкидывает влияние внесений/выводов.
    Ожидает колонки: date, value, gross_invested, gross_withdrawn.
    Возвращает число (0.1234 = 12.34%) или None.
    """
    df = df_one_ticker.copy()
    df["date"] = pd.to_datetime(df["date"])

    df = df[(df["date"] >= start_date) & (df["date"] <= end_date)].sort_values("date")

    if len(df) < 2:
        return None

    df["cashflow"] = (df["gross_invested"] - df["gross_withdrawn"]).fillna(0)
    df["value_prev"] = df["value"].shift(1)
    df["denom"] = df["value_prev"] + df["cashflow"]

    # дневная доходность
    df["ret"] = 0.0
    mask = df["denom"] > 0
    df.loc[mask, "ret"] = (df.loc[mask, "value"] - df.loc[mask, "denom"]) / df.loc[mask, "denom"]

    # перемножаем
    twr = (1 + df["ret"]).prod() - 1
    return float(twr)


# ---------- 5. Построение stats для всех тикеров ----------

def build_positions_stats(
        df_ts: pd.DataFrame,
        as_of: pd.Timestamp,
        periods: Optional[Dict[str, int]] = None,
) -> List[dict]:
    """
    df_ts — уже дневной таймсерис по ВСЕМ тикерам до as_of.
    Возвращает список словарей по тикерам со всеми цифрами.
    """
    if periods is None:
        periods = {
            "1W": 7,
            "1M": 30,
            "3M": 90,
            "6M": 180,
            "1Y": 365,
        }

    df_ts = df_ts.copy()
    df_ts["date"] = pd.to_datetime(df_ts["date"])
    df_ts = df_ts[df_ts["date"] <= as_of].sort_values(["ticker", "date"])

    results: List[dict] = []

    for ticker, df_t in df_ts.groupby("ticker"):
        df_t = df_t.sort_values("date")

        last = df_t.iloc[-1]

        rec = {
            "ticker": ticker,
            "as_of": as_of.date().isoformat(),
            "currency": last.get("currency"),
            "market_value": float(last.get("value") or 0),
            "total_pnl": float(last.get("total_pnl") or 0),
            "cum_invested": float(last.get("cum_invested") or 0),
            "cum_withdrawn": float(last.get("cum_withdrawn") or 0),
            "periods": {},
        }

        if rec["cum_invested"] > 0:
            rec["total_pnl_pct"] = rec["total_pnl"] / rec["cum_invested"] * 100
        else:
            rec["total_pnl_pct"] = None

        for label, days in periods.items():
            cutoff = as_of - timedelta(days=days)

            df_before = df_t[df_t["date"] <= cutoff]
            if df_before.empty:
                rec["periods"][label] = None
                continue

            start_row = df_before.iloc[-1]

            twr = compute_twr_for_window(df_t, start_row["date"], as_of)

            pnl_abs = float(last["total_pnl"] - start_row["total_pnl"])

            mask_period = (df_t["date"] > start_row["date"]) & (df_t["date"] <= as_of)
            cash_in = float(df_t.loc[mask_period, "gross_invested"].sum())
            cash_out = float(df_t.loc[mask_period, "gross_withdrawn"].sum())

            rec["periods"][label] = {
                "start_date": start_row["date"].date().isoformat(),
                "end_date": as_of.date().isoformat(),
                "twr_pct": float(twr * 100) if twr is not None else None,
                "pnl_abs": pnl_abs,
                "cash_in": cash_in,
                "cash_out": cash_out,
                "mv_start": float(start_row["value"] or 0),
                "mv_end": float(last["value"] or 0),
            }

        results.append(rec)

    return results