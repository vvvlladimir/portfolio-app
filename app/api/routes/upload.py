from io import StringIO
from typing import List

import pandas as pd
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile

from app.api.dependencies import get_factory
from app.repositories.factory import RepositoryFactory
from app.core.logger import logger

router = APIRouter()


@router.post("/transactions/csv")
async def upload_transactions_csv(
        file: UploadFile = File(...),
        factory: RepositoryFactory = Depends(get_factory),
):
    """date, type, ticker, shares, value, currency"""
    try:
        content = await file.read()
        df = pd.read_csv(StringIO(content.decode("utf-8")))

        required = {"date", "type", "ticker", "shares", "value", "currency"}
        if not required.issubset(set(map(str.lower, df.columns))):
            raise HTTPException(400, detail=f"CSV must contain columns: {required}")

        cols = {c: c.lower() for c in df.columns}
        df = df.rename(columns=cols)

        rows = [
            {
                "date": pd.to_datetime(r.date).date(),
                "type": str(r.type).upper(),
                "ticker": str(r.ticker).upper(),
                "shares": float(r.shares),
                "value": float(r.value),
                "currency": str(r.currency).upper(),
            }
            for r in df.itertuples(index=False)
        ]

        repo = factory.get_transaction_repository()
        inserted = repo.upsert_bulk(rows)

        return {"status": "ok", "inserted": inserted}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"upload_transactions_csv failed: {e}", exc_info=True)
        raise HTTPException(500, detail="Failed to upload transactions CSV")