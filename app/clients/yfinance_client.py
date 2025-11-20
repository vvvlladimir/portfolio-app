from datetime import date
from typing import List
import pandas as pd
import yfinance as yf
import time

from app.core.logger import logger


def fetch_prices(tickers: List[str], period="5y", interval="1d") -> pd.DataFrame:
    """Fetch historical prices for tickers via Yahoo Finance."""
    data = yf.download(
        tickers,
        period=period,
        interval=interval,
        progress=False,
        prepost=True,
        end=date.today().isoformat(),
    )
    if data.empty:
        raise ValueError("No data found")

    data = data.stack().reset_index()
    logger.info(f"Fetched {len(data)} rows of price data")
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
        logger.info(f"Fetched {len(result)} rows for {ticker}")
        return result

    except Exception as e:
        logger.error(f"Error fetching info for {ticker}: {e}")
        return {
            "ticker": ticker.upper(),
            "currency": "USD",
            "long_name": ticker.upper(),
            "exchange": None,
            "asset_type": "UNKNOWN",
        }
