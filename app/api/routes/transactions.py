from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from app.api.dependencies import get_factory
from app.managers.cache_manager import CacheManager
from app.repositories.factory import RepositoryFactory
from app.schemas.transactions import TransactionsOut
from app.core.logger import logger

router = APIRouter()
cache = CacheManager(prefix="transactions")

@router.get("/", response_model=List[TransactionsOut])
def list_transactions(
        ticker: Optional[str] = None,
        type: Optional[str] = Query(default=None, description="BUY/SELL/…"),
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        include_ticker_info: bool = True,
        factory: RepositoryFactory = Depends(get_factory),
):
    """Get transactions filtered by optional parameters."""
    try:
        cached = cache.get(ticker, type, date_from, date_to, include_ticker_info)
        if cached:
            return [TransactionsOut.model_validate(r) for r in cached]
        repo = factory.get_transaction_repository()
        rows = repo.get_by_filters(
            ticker=ticker,
            type=type,
            date_from=date_from,
            date_to=date_to,
            include_ticker_info=include_ticker_info,
        )
        records = [TransactionsOut.model_validate(r).model_dump(mode="json") for r in rows]

        cache.set(records, ticker, type, date_from, date_to, include_ticker_info, ttl=300)
        return records
    except Exception as e:
        logger.error(f"list_transactions failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to list transactions")