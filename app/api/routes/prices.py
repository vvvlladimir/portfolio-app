from datetime import date, timedelta
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.dependencies import get_factory
from app.managers.cache_manager import CacheManager
from app.repositories.factory import RepositoryFactory
from app.core.logger import logger
from app.schemas.prices import PricesOut
from app.tasks.refresh import refresh_market_data_task

router = APIRouter()
cache = CacheManager(prefix="prices")

@router.get("/", response_model=List[PricesOut])
def list_prices(
        tickers: Optional[List[str]] = Query(default=None),
        date_from: Optional[date] = (date.today() - timedelta(days=1)),
        date_to: Optional[date] = date.today(),
        factory: RepositoryFactory = Depends(get_factory),
):
    """Get price rows filtered by optional tickers and date range."""
    try:
        cached = cache.get(tickers, date_from, date_to)
        if cached:
            return [PricesOut.model_validate(r) for r in cached]

        repo = factory.get_price_repository()
        rows = repo.get_prices_by_filters(tickers=tickers, date_from=date_from, date_to=date_to)
        records = [PricesOut.model_validate(r).model_dump(mode="json") for r in rows]

        cache.set(records, tickers, date_from, date_to, ttl=300)
        return records
    except Exception as e:
        logger.error(f"list_prices failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to list prices")


@router.post("/refresh")
def refresh_market_data():
    """
    Trigger a background task to refresh market data for all tickers.
    """
    try:
        task = refresh_market_data_task.delay()
        return {
            "status": "success",
            "message": "Market data refresh has been queued.",
            "task_id": task.id
        }
    except Exception as e:
        logger.error(f"Failed to queue refresh_market_data task: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to queue market data refresh")
