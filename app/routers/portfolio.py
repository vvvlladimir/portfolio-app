from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session
from starlette import status

from app.database.upsert_data import upsert_portfolio_history, upsert_positions
from app.database.utils import refresh_materialized_view
from app.models.schemas import TransactionsOut, PositionsOut
from app.services.portfolio_service import get_transactions
from app.database.database import get_db, Position

router = APIRouter(prefix="/portfolio", tags=["portfolio"])


@router.get("/history")
async def get_portfolio_history(db: Session = Depends(get_db)):
    try:
        upsert_positions(db)
        data = upsert_portfolio_history(db)
        return {
            "status": "ok",
            "data": data
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.get("/transactions", response_model=list[TransactionsOut])
async def get_transactions_history(db: Session = Depends(get_db)):
    try:
        data = get_transactions(db)
        if not data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No transactions found"
            )
        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error: {str(e)}"
        )
@router.get("/last-positions", response_model=list[PositionsOut])
async def get_transactions_history(db: Session = Depends(get_db)):
    try:
        last_date = db.query(func.max(Position.date))
        data = db.query(Position).filter(Position.date == last_date).all()

        if not data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No transactions found"
            )

        return data

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error: {str(e)}"
        )


@router.get("/refresh-view")
async def refresh_view(
        view_name: str,
        concurrently: bool = True,
        db: Session = Depends(get_db),
):
    """
    Updates the materialized view.
    """
    try:
        refresh_materialized_view(db, view_name, concurrently)
        return {
            "status": "success",
            "view": view_name,
            "concurrently": concurrently,
        }
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except RuntimeError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        )