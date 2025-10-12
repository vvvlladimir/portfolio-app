from datetime import date
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.dependencies import get_factory
from app.repositories.factory import RepositoryFactory
from app.core.logger import logger

router = APIRouter()


@router.get("/")
def list_prices(
        tickers: Optional[List[str]] = Query(default=None),
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        factory: RepositoryFactory = Depends(get_factory),
):
    """Get price rows filtered by optional tickers and date range."""
    try:
        repo = factory.get_price_repository()
        rows = repo.get_prices_by_filters(tickers=tickers, date_from=date_from, date_to=date_to)
        return rows
    except Exception as e:
        logger.error(f"list_prices failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to list prices")


@router.post("/refresh")
def refresh_market_data(
        factory: RepositoryFactory = Depends(get_factory),
):
    """Refresh market data for all tickers in the database."""
    try:
        ticker_repo = factory.get_ticker_repository()
        price_repo = factory.get_price_repository()

        tickers = [ticker.ticker for ticker in ticker_repo.get_tickers()]
        if not tickers:
            raise HTTPException(400, detail="No tickers in database")

        from app.clients.yfinance_client import fetch_prices

        try:
            price_data = fetch_prices(tickers)
            if price_data is not None:
                inserted = price_repo.upsert_bulk(price_data)
                logger.info(f"Updated {inserted} price records for {tickers}")
            else:
                logger.warning(f"No price data available for {tickers}")
        except Exception as e:
            logger.error(f"Failed to refresh prices for {tickers}: {e}")

        result = {
            "status": "success",
            "total_tickers": len(tickers),
        }
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"refresh_market_data failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to refresh prices")