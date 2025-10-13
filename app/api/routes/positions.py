from datetime import date
from typing import List
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, cast, Float
from app.api.dependencies import get_factory
from app.models import Price, TickerInfo, Transaction, Position
from app.repositories.factory import RepositoryFactory
from app.schemas.positions import PositionsOut
from app.core.logger import logger
from app.services.positions_service import calculate_positions, get_snapshot_positions

router = APIRouter()


@router.get("/", response_model=List[PositionsOut])
def list_positions(factory: RepositoryFactory = Depends(get_factory)):
    """Return all positions."""
    try:
        repo = factory.get_position_repository()
        rows = repo.get_all()
        return [PositionsOut.model_validate(r) for r in rows]
    except Exception as e:
        logger.error(f"list_positions failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to list positions")


@router.get("/snapshot", response_model=List[PositionsOut])
def snapshot_positions(
        as_of: date = date.today(),
        factory: RepositoryFactory = Depends(get_factory)
):
    """Return a snapshot of current positions with market values."""
    try:
        price_repo = factory.get_price_repository()
        stmt = select(Price)
        df_prices = pd.read_sql(stmt, price_repo.db.bind)

        pos_repo = factory.get_position_repository()
        stmt = select(Position, TickerInfo).join(TickerInfo)
        df_positions = pd.read_sql(stmt, pos_repo.db.bind)

        data = get_snapshot_positions(df_positions, df_prices, as_of)

        data["ticker_info"] = data.apply(
            lambda r: {
                "ticker": r["ticker_1"],
                "currency": r["currency"],
                "long_name": r["long_name"],
                "exchange": r["exchange"],
                "asset_type": r["asset_type"],
            },
            axis=1
        )
        data = data.drop(columns=["ticker_1", "currency", "long_name", "exchange", "asset_type"])
        return [PositionsOut.model_validate(r) for r in data.to_dict(orient="records")]
    except Exception as e:
        logger.error(f"snapshot_positions failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to get positions snapshot")

@router.post("/rebuild")
def rebuild_positions(factory: RepositoryFactory = Depends(get_factory)):
    """Compute and rebuild the entire positions table from transactions and prices."""
    try:
        price_repo = factory.get_price_repository()
        stmt = (
            select(
                Price.date,
                Price.ticker,
                cast(Price.close, Float).label("close"),
                TickerInfo.currency
            )
            .join(TickerInfo, Price.ticker == TickerInfo.ticker)
        )
        result = price_repo.db.execute(stmt)
        columns = [col.key for col in stmt.selected_columns]
        df_prices = pd.DataFrame(result.all(), columns=columns)

        tx_repo = factory.get_transaction_repository()
        stmt = (
            select(
                Transaction.date,
                Transaction.type,
                Transaction.ticker,
                Transaction.currency,
                cast(Transaction.shares, Float).label("shares"),
                cast(Transaction.value, Float).label("value")
            )
        )
        result = tx_repo.db.execute(stmt)
        columns = [col.key for col in stmt.selected_columns]
        df_transactions = pd.DataFrame(result.all(), columns=columns)

        data = calculate_positions(df_transactions, df_prices, factory)

        pos_repo = factory.get_position_repository()
        pos_repo.delete_all()
        pos_repo.upsert_bulk(data)

        return {"status": "ok", "rows": len(data)}
    except Exception as e:
        logger.error(f"rebuild_positions failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to rebuild positions")
