from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException
from app.api.dependencies import get_factory
from app.managers.cache_manager import CacheManager
from app.repositories.factory import RepositoryFactory
from app.core.logger import logger
from app.schemas.ticker import TickerOut
from app.clients.yfinance_client import fetch_ticker_info
from app.services.fx_rates_service import FXRateService

router = APIRouter()
cache = CacheManager(prefix="tickers")

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
        cached = cache.get(exchange, currency, asset_type)
        if cached:
            return [TickerOut.model_validate(r) for r in cached]

        repo = factory.get_ticker_repository()
        rows = repo.get_tickers(exchange=exchange, currency=currency, asset_type=asset_type)
        records = [TickerOut.model_validate(r).model_dump(mode="json") for r in rows]

        cache.set(records, exchange, currency, asset_type, ttl=300)
        return records
    except Exception as e:
        logger.error(f"list_tickers failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to list tickers")


@router.post("/refresh")
def refresh_tickers_info(
        factory: RepositoryFactory = Depends(get_factory),
):
    """
    Ensure that all tickers from transactions exist in the database, fetching missing ones
    """
    try:
        ticker_repo = factory.get_ticker_repository()
        existing_currencies = set(ticker.currency for ticker in ticker_repo.get_tickers())

        transaction_repo = factory.get_transaction_repository()
        requested_tickers = set(transaction_repo.get_all_tickers())
        requested_currencies = set(transaction_repo.get_transaction_currencies())

        fx_rates, fx_missing = FXRateService(factory).get_needed_pairs(
            requested_currencies | existing_currencies
        )

        missing = requested_tickers | set(fx_missing)

        if not missing:
            cache.clear()
            return {"status": "ok", "inserted": 0, "missing": missing}

        inserted = ticker_repo.upsert_bulk_missing(missing)

        cache.clear()
        return {"status": "ok", "inserted": inserted, "missing": missing}
    except Exception as e:
        logger.error(f"refresh_tickers_info failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to refresh tickers info")
