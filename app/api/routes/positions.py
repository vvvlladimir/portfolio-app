from datetime import date, timedelta
from typing import List

import numpy as np
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from app.api.dependencies import get_factory
from app.managers.cache_manager import CacheManager
from app.models import Price, TickerInfo, Transaction, Position
from app.repositories.factory import RepositoryFactory
from app.schemas.positions import PositionsOut, PositionsStatsOut
from app.core.logger import logger
from app.services.positions_service import calculate_positions, get_snapshot_positions, build_positions_stats

router = APIRouter()
cache = CacheManager(prefix="positions")


@router.get("/", response_model=List[PositionsOut])
def list_positions(factory: RepositoryFactory = Depends(get_factory)):
    """Return all positions."""
    try:
        cached = cache.get()
        if cached:
            return [PositionsOut.model_validate(r) for r in cached]

        repo = factory.get_position_repository()
        rows = repo.get_all()

        records = [PositionsOut.model_validate(r).model_dump(mode="json") for r in rows]
        cache.set(records, ttl=300)
        return records
    except Exception as e:
        logger.error(f"list_positions failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to list positions")


@router.get("/snapshot", response_model=List[PositionsOut])
def snapshot_positions(
        date_to: date = date.today(),
        expand_daily: bool = False,
        get_last: bool = False,
        factory: RepositoryFactory = Depends(get_factory)
):
    """Return a snapshot of current positions with market values."""
    try:
        cached = cache.get("snapshot", date_to.isoformat(), expand_daily, get_last)
        if cached:
            return [PositionsOut.model_validate(r) for r in cached]

        price_repo = factory.get_price_repository()
        stmt = select(Price)
        df_prices = pd.read_sql(stmt, price_repo.db.bind)

        pos_repo = factory.get_position_repository()
        stmt = select(Position)
        df_positions = pd.read_sql(stmt, pos_repo.db.bind)

        data = get_snapshot_positions(df_positions, df_prices, date_to, expand_daily, get_last)
        records = data.to_dict(orient="records")

        cache.set(records, "snapshot", date_to.isoformat(), expand_daily, get_last, ttl=300)

        return [PositionsOut.model_validate(r) for r in records]
    except Exception as e:
        logger.error(f"snapshot_positions failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to get positions snapshot")

@router.get("/stats")
def positions_stats(
        date_to: date = date.today(),
        factory: RepositoryFactory = Depends(get_factory),
):
    """
    Return statistics about positions as of a specific date.
    """
    try:
        cached = cache.get("stats", date_to.isoformat())
        if cached:
            return cached

        price_repo = factory.get_price_repository()
        pos_repo = factory.get_position_repository()

        df_prices = pd.read_sql(
            select(Price, TickerInfo).join(TickerInfo).where(Price.date <= date_to),
            price_repo.db.bind,
        )

        df_positions = pd.read_sql(
            select(Position).where(Position.date <= date_to),
            pos_repo.db.bind,
        )

        df_ts = get_snapshot_positions(
            df_positions,
            df_prices,
            date_to=pd.Timestamp(date_to),
            expand_daily=True,
        )

        records = build_positions_stats(df_ts, pd.Timestamp(date_to))

        cache.set(records, "stats", date_to.isoformat(), ttl=600)

        return records

    except Exception as e:
        logger.error(f"positions_stats failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to calculate stats")

@router.post("/rebuild")
def rebuild_positions(factory: RepositoryFactory = Depends(get_factory)):
    """Compute and rebuild the entire positions table from transactions and prices."""
    try:

        price_repo = factory.get_price_repository()
        stmt = select(Price, TickerInfo).join(TickerInfo)
        df_prices = pd.read_sql(stmt, price_repo.db.bind)

        tx_repo = factory.get_transaction_repository()
        stmt = select(Transaction)
        df_transactions = pd.read_sql(stmt, tx_repo.db.bind)

        data = calculate_positions(df_transactions, df_prices, factory)

        pos_repo = factory.get_position_repository()
        pos_repo.delete_all()
        pos_repo.upsert_bulk(data)

        cache.clear()
        return {"status": "ok", "rows": len(data)}
    except Exception as e:
        logger.error(f"rebuild_positions failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to rebuild positions")
