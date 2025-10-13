from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from app.api.dependencies import get_factory
from app.repositories.factory import RepositoryFactory
from app.schemas.transactions import TransactionsOut
from app.core.logger import logger

router = APIRouter()


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
        repo = factory.get_transaction_repository()
        rows = repo.get_by_filters(
            ticker=ticker,
            type=type,
            date_from=date_from,
            date_to=date_to,
            include_ticker_info=include_ticker_info,
        )
        return [TransactionsOut.model_validate(r) for r in rows]
    except Exception as e:
        logger.error(f"list_transactions failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to list transactions")