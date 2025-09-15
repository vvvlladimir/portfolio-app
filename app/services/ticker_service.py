from typing import List
from itertools import combinations
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import yfinance as yf
import time
from app.database import Price, TickerInfo


def fetch_prices(tickers: List[str], period="5y", interval="1d") -> pd.DataFrame:
    """Fetch historical prices for tickers via Yahoo Finance."""
    data = yf.download(
        tickers,
        period=period,
        interval=interval,
        progress=False,
    )

    if data.empty:
        raise ValueError("No data found")

    data = data.stack().reset_index()
    return data


def upsert_prices(db: Session, data: pd.DataFrame) -> int:
    """Save DataFrame with prices to database using upsert by ticker+date."""
    try:
        inserted = 0
        upsert_missing_tickers_info(db, data["Ticker"].unique().tolist())

        for _, row in data.iterrows():
            stmt = insert(Price).values(
                ticker=row["Ticker"].upper(),
                date=row["Date"].date(),
                open=float(row["Open"]) if not pd.isna(row["Open"]) else None,
                high=float(row["High"]) if not pd.isna(row["High"]) else None,
                low=float(row["Low"]) if not pd.isna(row["Low"]) else None,
                close=float(row["Close"]) if not pd.isna(row["Close"]) else None,
                volume=float(row["Volume"]) if not pd.isna(row["Volume"]) else None
            )
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["ticker", "date"]
            )

            result = db.execute(stmt)
            if result.rowcount > 0:
                inserted += 1

        db.commit()
        return inserted
    except SQLAlchemyError as e:
        db.rollback()
        print("Error upserting prices:", str(e))
        raise


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


def upsert_ticker_info(db: Session, data: dict) -> None:
    """Insert or update ticker information in database."""
    try:
        stmt = insert(TickerInfo).values(**data)
        stmt = stmt.on_conflict_do_update(
            index_elements=["ticker"],
            set_={
                "currency": stmt.excluded.currency,
                "long_name": stmt.excluded.long_name,
                "exchange": stmt.excluded.exchange,
                "asset_type": stmt.excluded.asset_type,
            }
        )

        db.execute(stmt)
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        print("Error upserting ticker info for", data.get("ticker"), ":", str(e))
        raise

def upsert_missing_tickers_info(db: Session, tickers: List[str]) -> int:
    """Fetch and upsert ticker info for tickers missing in the database."""
    valid_tickers = [str(t).upper() for t in tickers if pd.notna(t) and str(t).strip()]
    tickers_set = set(valid_tickers)

    if not tickers_set:
        return 0

    existing = set(
        t[0] for t in db.query(TickerInfo.ticker)
        .filter(TickerInfo.ticker.in_(tickers_set))
        .all()
    )

    to_fetch = tickers_set - existing
    for ticker in to_fetch:
        data = fetch_ticker_info(ticker)
        upsert_ticker_info(db, data)

    return len(to_fetch)

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