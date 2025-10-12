from typing import List, Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import cast, Float, select

from app.api.dependencies import get_factory
from app.models import Price, Position, TickerInfo
from app.repositories.factory import RepositoryFactory
from app.schemas.portfolio import PortfolioHistoryOut
from app.core.logger import logger
from app.services.portfolio_service import calculate_portfolio_history

router = APIRouter()


@router.get("/history", response_model=List[PortfolioHistoryOut])
def get_portfolio_history(factory: RepositoryFactory = Depends(get_factory)):
    try:
        repo = factory.get_portfolio_history_repository()
        rows = repo.get_all()
        return [PortfolioHistoryOut.model_validate(r) for r in rows]
    except Exception as e:
        logger.error(f"get_portfolio_history failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to fetch portfolio history")


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
        stmt = (
            select(
                Price.date,
                Price.ticker,
                cast(Price.close, Float).label("close")
            )
        )
        result = price_repo.db.execute(stmt)
        columns = [col.key for col in stmt.selected_columns]
        df_prices = pd.DataFrame(result.all(), columns=columns)

        pos_repo = factory.get_position_repository()
        stmt = (
            select(
                Position.date,
                Position.ticker,
                cast(Position.shares, Float).label("shares"),
                cast(Position.close, Float).label("close"),
                cast(Position.gross_invested, Float).label("gross_invested"),
                cast(Position.gross_withdrawn, Float).label("gross_withdrawn"),
                TickerInfo.currency
            )
            .join(TickerInfo, Position.ticker == TickerInfo.ticker)
        )

        result = pos_repo.db.execute(stmt)
        columns = [col.key for col in stmt.selected_columns]
        df_positions = pd.DataFrame(result.all(), columns=columns)

        df = calculate_portfolio_history(df_positions, df_prices, base_currency=base_currency, factory=factory)

        hist_repo = factory.get_portfolio_history_repository()
        hist_repo.delete_all()
        inserted = hist_repo.upsert_bulk(df)
        return {"status": "ok", "rows": inserted, "base_currency": base_currency}
    except Exception as e:
        logger.error(f"rebuild_portfolio_history failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to rebuild portfolio history")