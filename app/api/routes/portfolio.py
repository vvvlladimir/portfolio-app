from pprint import pprint
from typing import List, Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import cast, Float, select

from app.api.dependencies import get_factory
from app.managers.cache_manager import CacheManager
from app.models import Price, Position, TickerInfo
from app.repositories.factory import RepositoryFactory
from app.schemas.portfolio import PortfolioHistoryOut, PortfolioHistoryResponse, PortfolioWeightsResponse
from app.core.logger import logger
from app.services.portfolio_service import calculate_portfolio_history, calculate_portfolio_weights
from app.services.positions_service import get_snapshot_positions

router = APIRouter()
cache = CacheManager(prefix="portfolio")

@router.get("/history", response_model=PortfolioHistoryResponse)
def get_portfolio_history(factory: RepositoryFactory = Depends(get_factory)):
    try:
        cached = cache.get()
        if cached:
            return PortfolioHistoryResponse(
                currency="USD",
                history=[PortfolioHistoryOut.model_validate(r) for r in cached]
            )

        repo = factory.get_portfolio_history_repository()
        rows = repo.get_all()

        records = [PortfolioHistoryOut.model_validate(r).model_dump(mode="json") for r in rows]
        cache.set(records, ttl=300)

        return PortfolioHistoryResponse(
            currency="USD",
            history=[PortfolioHistoryOut.model_validate(r) for r in rows]
        )
    except Exception as e:
        logger.error(f"get_portfolio_history failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to fetch portfolio history")

@router.get("/weights", response_model=PortfolioWeightsResponse)
def get_portfolio_weights(
        get_last: bool = Query(True, description="Return only latest weights or full history"),
        factory: RepositoryFactory = Depends(get_factory),
):
    """
    Returns either:
    - latest weights per ticker: [{ticker, weight}]
    - full historical weights in wide format: [{date, TICKER1, TICKER2, ...}]
    """

    try:
        cached = cache.get("weights", get_last)
        if cached:
            return cached

        price_repo = factory.get_price_repository()
        stmt = select(Price)
        df_prices = pd.read_sql(stmt, price_repo.db.bind)

        pos_repo = factory.get_position_repository()
        stmt = select(Position, TickerInfo).join(TickerInfo)
        df_positions = pd.read_sql(stmt, pos_repo.db.bind)

        data = calculate_portfolio_weights(
            df_positions=df_positions,
            df_prices=df_prices,
            latest=get_last,
            factory=factory
        )

        tickers = data.columns[data.columns != "date"].tolist()
        weights_matrix = data[tickers].to_numpy(dtype=float)
        dates = data["date"].tolist()
        rows = [
            {"date": d, "weights": w.tolist()}
            for d, w in zip(dates, weights_matrix)
        ]
        return PortfolioWeightsResponse(
            tickers=tickers,
            rows=rows,
        )
    except Exception as e:
        logger.error(f"get_portfolio_weights failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to fetch portfolio weights")

@router.post("/history/rebuild")
def rebuild_portfolio_history(
        base_currency: str = Query(default="USD"),
        factory: RepositoryFactory = Depends(get_factory),
):
    """
    Compute and rebuild the entire portfolio history from positions and prices.
    """
    try:
        price_repo = factory.get_price_repository()
        stmt = select(Price)
        df_prices = pd.read_sql(stmt, price_repo.db.bind)

        pos_repo = factory.get_position_repository()
        stmt = select(Position, TickerInfo).join(TickerInfo)
        df_positions = pd.read_sql(stmt, pos_repo.db.bind)

        df = calculate_portfolio_history(df_positions, df_prices, base_currency=base_currency, factory=factory)

        hist_repo = factory.get_portfolio_history_repository()
        hist_repo.delete_all()
        inserted = hist_repo.upsert_bulk(df)
        cache.clear()

        return {"status": "ok", "rows": inserted, "base_currency": base_currency}
    except Exception as e:
        logger.error(f"rebuild_portfolio_history failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to rebuild portfolio history")