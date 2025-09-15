from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.services.portfolio_service import calculate_portfolio_history, calculate_positions
from app.database import get_db

router = APIRouter(prefix="/portfolio", tags=["portfolio"])


@router.get("/history")
async def get_portfolio_history(db: Session = Depends(get_db)):
    """
    Получает историю портфеля с расчетом всех метрик
    """
    try:
        calculate_positions(db)
        result = calculate_portfolio_history(db)
        # print(result.head())
        return result
    except Exception as e:
        db.rollback()
        return {"status": "error", "message": str(e)}
