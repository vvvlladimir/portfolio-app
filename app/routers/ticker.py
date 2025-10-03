from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database.database import get_db
from app.database.upsert_data import upsert_prices, upsert_all_prices, upsert_ticker_info
from app.models.schemas import TickersRequest
from app.services.ticker_service import fetch_prices, fetch_ticker_info

router = APIRouter(prefix="/ticker", tags=["ticker"])


@router.post("/load-prices/{ticker}")
async def load_prices(req: TickersRequest, db: Session = Depends(get_db)):
    """
    Загружает цены для указанных тикеров
    """
    try:
        tickers = [t.upper() for t in req.tickers if len(t) <= 10]
        if not tickers:
            raise ValueError("No valid tickers provided")

        data = fetch_prices(tickers)
        return {"status": "ok", "tickers": tickers, "rows_inserted": upsert_prices(db, data)}

    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.post("/load-prices-all")
async def load_all_prices(db: Session = Depends(get_db)):
    """
    Загружает цены для всех тикеров из транзакций
    """
    try:
        return {
            "status": "ok",
            **upsert_all_prices(db)
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.post("/load-info")
async def load_tickers_info(req: TickersRequest, db: Session = Depends(get_db)):
    """
    Загружает информацию по тикерам из Yahoo Finance
    """
    try:
        tickers = [t.upper() for t in req.tickers if len(t) <= 10]
        if not tickers:
            raise HTTPException(status_code=400, detail="No valid tickers provided")

        for ticker in tickers:
            data = fetch_ticker_info(ticker)
            upsert_ticker_info(db, data)

        return {"status": "ok", "tickers": tickers}

    except Exception as e:
        return {"status": "error", "message": str(e)}
