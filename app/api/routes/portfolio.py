from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.dependencies import get_factory
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
        pos_repo = factory.get_position_repository()
        hist_repo = factory.get_portfolio_history_repository()

        price_rows = price_repo.get_prices_rows()
        pos_rows = pos_repo.get_all()

        df = calculate_portfolio_history(pos_rows, price_rows, base_currency=base_currency)

        rows_to_insert = [
            {
                "date": r.date.date(),
                "total_value": r.total_value,
                "invested_value": r.invested_value,
                "gross_invested": r.gross_invested,
                "gross_withdrawn": r.gross_withdrawn,
            }
            for r in df.itertuples(index=False)
        ]

        hist_repo.delete_all()
        hist_repo.bulk_insert_history(rows_to_insert)

        return {"status": "ok", "rows": len(rows_to_insert), "base_currency": base_currency}
    except Exception as e:
        logger.error(f"rebuild_portfolio_history failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to rebuild portfolio history")