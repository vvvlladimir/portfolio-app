from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session
from starlette import status
from app.models.schemas import TransactionsOut, PositionsOut

from app.services.portfolio_service import upsert_portfolio_history, calculate_positions, get_transactions, \
    upsert_positions
from app.database import get_db, Position

router = APIRouter(prefix="/portfolio", tags=["portfolio"])


@router.get("/history")
async def get_portfolio_history(db: Session = Depends(get_db)):
    """
    Получает историю портфеля с расчетом всех метрик
    """
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