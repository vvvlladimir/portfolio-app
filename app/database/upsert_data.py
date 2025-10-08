from typing import List
from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
from app.database.database import Price, TickerInfo, Transaction, PortfolioHistory, Position, get_db
from app.models.schemas import PortfolioHistoryOut, PositionsOut
from app.services.portfolio_service import calculate_portfolio_history, calculate_positions
from app.services.ticker_service import fetch_prices, fetch_fx_rates, fetch_ticker_info


def upsert_prices(db: Session, data: pd.DataFrame) -> int:
    """Save DataFrame with prices to database using upsert by ticker+date."""
    try:
        inserted = 0

        last_dates = (
            db.query(Price.ticker, func.max(Price.date))
            .group_by(Price.ticker)
            .all()
        )
        last_date_map = {t: d for t, d in last_dates}

        for _, row in data.iterrows():
            ticker = row["Ticker"].upper()
            date_value = row["Date"].date()

            if ticker in last_date_map and date_value <= last_date_map[ticker]:
                continue

            stmt = insert(Price).values(
                ticker=ticker,
                date=date_value,
                open=float(row["Open"]) if not pd.isna(row["Open"]) else None,
                high=float(row["High"]) if not pd.isna(row["High"]) else None,
                low=float(row["Low"]) if not pd.isna(row["Low"]) else None,
                close=float(row["Close"]) if not pd.isna(row["Close"]) else None,
                volume=float(row["Volume"]) if not pd.isna(row["Volume"]) else None
            ).on_conflict_do_nothing(
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

def upsert_all_prices(db: Session) -> int:
    try:
        tickers_db = db.query(TickerInfo.ticker).distinct().all()
        tickers = [t[0].upper() for t in tickers_db]

        if not tickers:
            return {"status": "error", "message": "No tickers in transactions table"}

        tx_currencies = set(c[0] for c in db.query(Transaction.currency).distinct())
        ticker_currencies = set(c[0] for c in db.query(TickerInfo.currency).distinct())
        currencies = sorted(tx_currencies | ticker_currencies)

        price_data = fetch_prices(tickers)
        rows_inserted_tickers = upsert_prices(db, price_data)

        fx_data, pairs = fetch_fx_rates(currencies)
        upsert_missing_tickers_info(db, pairs)
        rows_inserted_fx = upsert_prices(db, fx_data)

        return {
            "tickers": tickers,
            "currencies": currencies,
            "rows_inserted": rows_inserted_tickers + rows_inserted_fx
        }
    except SQLAlchemyError as e:
        db.rollback()
        print("Error upserting prices:", str(e))
        raise

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

def upsert_portfolio_history(db: Session, base_currency: str = "USD") -> int:
    try:
        inserted = 0
        data = calculate_portfolio_history(db, base_currency)

        db.query(PortfolioHistory).delete()
        for _, row in data.iterrows():
            stmt = insert(PortfolioHistory).values(
                date=row["date"].date(),
                total_value=float(row["total_value"]) if not pd.isna(row["total_value"]) else None,
                invested_value=float(row["invested_value"]) if not pd.isna(row["invested_value"]) else None,
                gross_invested=float(row["gross_invested"]) if not pd.isna(row["gross_invested"]) else None,
                gross_withdrawn=float(row["gross_withdrawn"]) if not pd.isna(row["gross_withdrawn"]) else None
            )
            db.execute(stmt)
            inserted += 1
        db.commit()

        rows = db.query(PortfolioHistory).all()

        return [PortfolioHistoryOut.model_validate(row) for row in rows]
    except SQLAlchemyError as e:
        db.rollback()
        print("Error upserting portfolio history:", str(e))
        raise

def upsert_positions(db: Session) -> int:
    try:
        data = calculate_positions(db)
        db.query(Position).delete()
        rows_to_insert = [
            {
                "date": row["date"].date(),
                "ticker": row["ticker"],
                "shares": row["shares"],
                "close": row["close"],
                "gross_invested": row["gross_invested"],
                "gross_withdrawn": row["gross_withdrawn"],
                "total_pnl": row["total_pnl"],
            }
            for _, row in data.iterrows()
        ]

        db.bulk_insert_mappings(Position, rows_to_insert)
        db.commit()

        rows = db.query(Position).all()
        return [PositionsOut.model_validate(row) for row in rows]
    except SQLAlchemyError as e:
        db.rollback()
        print("Error upserting portfolio history:", str(e))
        raise
