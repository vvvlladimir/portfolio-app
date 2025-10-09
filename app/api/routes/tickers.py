from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from app.api.dependencies import get_factory
from app.repositories.factory import RepositoryFactory
from app.core.logger import logger
from app.schemas.ticker import TickersRequest, TickerOut

router = APIRouter()


@router.get("/", response_model=List[TickerOut])
def list_tickers(
        exchange: Optional[str] = None,
        currency: Optional[str] = None,
        asset_type: Optional[str] = None,
        factory: RepositoryFactory = Depends(get_factory),
):
    """
    Get a list of tickers with optional filters.
    """
    try:
        repo = factory.get_ticker_repository()
        rows = repo.get_tickers(exchange=exchange, currency=currency, asset_type=asset_type)
        return [TickerOut.model_validate(r) for r in rows]
    except Exception as e:
        logger.error(f"list_tickers failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to list tickers")


@router.post("/ensure")
def ensure_tickers_info(
        tickers: Optional[List[str]] = Query(default=None),
        factory: RepositoryFactory = Depends(get_factory),
):
    """
    Ensure that the given tickers exist in the database, fetching missing ones
    """
    try:
        repo = factory.get_ticker_repository()

        existing_tickers = repo.get_tickers()
        existing_symbols = {ticker.ticker for ticker in existing_tickers}

        if not tickers:
            requested_symbols = set(factory.get_transaction_repository().get_all_tickers())
        else:
            requested_symbols = set(tickers)
        missing = requested_symbols - existing_symbols
        if not missing:
            return {"status": "ok", "inserted": 0, "missing": []}

        from app.clients.yfinance_client import fetch_ticker_info
        rows = [fetch_ticker_info(t) for t in missing]
        inserted = repo.bulk_insert_tickers(rows)
        return {"status": "ok", "inserted": inserted, "missing": list(missing)}
    except Exception as e:
        logger.error(f"ensure_tickers_info failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to ensure tickers info")