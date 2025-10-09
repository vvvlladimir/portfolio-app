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
    """
    Вернуть цены с опциональными фильтрами.
    """
    try:
        repo = factory.get_price_repository()
        rows = repo.get_prices_rows(tickers=tickers, date_from=date_from, date_to=date_to)
        # Возвращаем как есть (ORM объекты сериализуются автоматически, если поля простые)
        # при желании можно сделать отдельные схемы для прайсов
        return rows
    except Exception as e:
        logger.error(f"list_prices failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to list prices")


@router.post("/refresh")
def refresh_market_data(
        factory: RepositoryFactory = Depends(get_factory),
):
    """
    Простой пример «обновления рынка»: берём все тикеры из БД и апдейтим их котировки+FX.
    Тут подразумевается, что у тебя есть сервис ingestion (если нет — можно прямо тут вызывать clients+yfinance).
    """
    try:
        ticker_repo = factory.get_ticker_repository()
        price_repo = factory.get_price_repository()

        tickers = [t.ticker for t in ticker_repo.get_all()]
        if not tickers:
            raise HTTPException(400, detail="No tickers in database")

        # Ниже — псевдо-логика, если у тебя есть services/clients.
        # Если их ещё нет, вызвать напрямую твой clients.yfinance_client
        # и затем price_repo.upsert_prices_bulk(...)
        # from app.clients.yfinance_client import fetch_prices, fetch_fx_rates
        # df = fetch_prices(tickers)
        # inserted = price_repo.upsert_prices_bulk(df_normalized_records)

        return {"status": "ok", "tickers": tickers}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"refresh_market_data failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to refresh prices")