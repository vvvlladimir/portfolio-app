from typing import List
from itertools import combinations
import pandas as pd
import yfinance as yf
import time


def fetch_prices(tickers: List[str], period="5y", interval="1d") -> pd.DataFrame:
    """Fetch historical prices for tickers via Yahoo Finance."""
    data = yf.download(
        tickers,
        period=period,
        interval=interval,
        progress=False,
        prepost=True,
    )
    if data.empty:
        raise ValueError("No data found")

    data = data.stack().reset_index()
    return data



def fetch_ticker_info(ticker: str, sleep: float = 0.5) -> dict:
    """Fetch ticker information via yfinance with fallback and rate limiting."""
    try:
        t = yf.Ticker(ticker)
        info = t.info or {}
        fast_info = t.fast_info or {}

        result = {
            "ticker": ticker.upper(),
            "currency": info.get("currency") or fast_info.get("currency") or "USD",
            "long_name": info.get("longName") or info.get("shortName") or ticker.upper(),
            "exchange": info.get("exchange") or fast_info.get("exchange") or None,
            "asset_type": info.get("quoteType") or fast_info.get("quoteType") or "UNKNOWN",
        }

        if sleep > 0:
            time.sleep(sleep)
        return result

    except Exception as e:
        print("Error fetching info for", ticker, ":", str(e))
        return {
            "ticker": ticker.upper(),
            "currency": "USD",
            "long_name": ticker.upper(),
            "exchange": None,
            "asset_type": "UNKNOWN",
        }



def fetch_fx_rates(currencies: List[str], period="5y", interval="1d") -> pd.DataFrame:
    """Fetch all available unique FX rates for given currencies"""

    pairs = [f"{a}{b}=X" for a, b in combinations(currencies, 2)]

    if not pairs:
        return pd.DataFrame()

    data = yf.download(
        pairs,
        period=period,
        interval=interval,
        progress=False,
    )

    if data.empty:
        raise ValueError("No FX data found")

    data = data.stack().reset_index()
    return data


"""
CREATE MATERIALIZED VIEW positions_mv AS
WITH tx_daily AS (
    SELECT
        t.ticker,
        t.date,
        SUM(
            CASE
                WHEN t.type = 'BUY'  THEN t.shares
                WHEN t.type = 'SELL' THEN -t.shares
                ELSE 0
            END
        ) AS signed_shares
    FROM transactions t
    GROUP BY t.ticker, t.date
),
calendar AS (
    SELECT gs::date AS date
    FROM generate_series(
        (SELECT MIN(date) FROM transactions),
        (SELECT MAX(date) FROM prices),
        interval '1 day'
    ) gs
),
grid AS (
    SELECT c.date, tk.ticker
    FROM calendar c
    CROSS JOIN (SELECT DISTINCT ticker FROM transactions) tk
),
pos AS (
    SELECT
        g.date,
        g.ticker,
        SUM(COALESCE(tx.signed_shares, 0))
            OVER (PARTITION BY g.ticker ORDER BY g.date) AS cum_shares
    FROM grid g
    LEFT JOIN tx_daily tx
        ON tx.ticker = g.ticker
       AND tx.date = g.date
),
px_ff AS (
    SELECT
        g.date,
        g.ticker,
        (
            SELECT p.close
            FROM prices p
            WHERE p.ticker = g.ticker
              AND p.date <= g.date
            ORDER BY p.date DESC
            LIMIT 1
        ) AS close_price_native
    FROM grid g
)
SELECT
    p.date,
    p.ticker,
    ti.currency AS ticker_currency,
    p.cum_shares,
    px.close_price_native
FROM pos p
JOIN px_ff px
  ON px.date = p.date AND px.ticker = p.ticker
JOIN tickers ti
  ON ti.ticker = p.ticker;
"""