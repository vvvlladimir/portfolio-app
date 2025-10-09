from typing import List
from fastapi import APIRouter, Depends, HTTPException

from app.api.dependencies import get_factory
from app.repositories.factory import RepositoryFactory
from app.schemas.positions import PositionsOut
from app.core.logger import logger
from app.services.positions_service import calculate_positions

router = APIRouter()


@router.get("/", response_model=List[PositionsOut])
def list_positions(factory: RepositoryFactory = Depends(get_factory)):
    """
    Return all positions.
    """
    try:
        repo = factory.get_position_repository()
        rows = repo.get_all()
        return [PositionsOut.model_validate(r) for r in rows]
    except Exception as e:
        logger.error(f"list_positions failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to list positions")


@router.post("/rebuild")
def rebuild_positions(factory: RepositoryFactory = Depends(get_factory)):
    """
    Compute and rebuild the entire positions table from transactions and prices.
    """
    try:
        tx_repo = factory.get_transaction_repository()
        price_repo = factory.get_price_repository()
        pos_repo = factory.get_position_repository()

        tx_rows = tx_repo.get_transactions_by_filters()
        price_rows = price_repo.get_prices_by_filters()

        df = calculate_positions(tx_rows, price_rows)

        rows_to_insert = [
            {
                "date": r.date.date(),
                "ticker": r.ticker,
                "shares": r.shares,
                "close": r.close,
                "gross_invested": r.gross_invested,
                "gross_withdrawn": r.gross_withdrawn,
                "total_pnl": r.total_pnl,
            }
            for r in df.itertuples(index=False)
        ]

        pos_repo.delete_all()
        pos_repo.bulk_insert_positions(rows_to_insert)

        return {"status": "ok", "rows": len(rows_to_insert)}
    except Exception as e:
        logger.error(f"rebuild_positions failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to rebuild positions")