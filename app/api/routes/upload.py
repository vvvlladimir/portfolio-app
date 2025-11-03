from io import StringIO
from typing import List

import dateparser
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

        cols = {c: c.lower() for c in df.columns}
        df = df.rename(columns=cols)

        required = {"date", "type", "ticker", "shares", "value", "currency"}
        if not required.issubset(set(map(str.lower, df.columns))):
            raise HTTPException(400, f"CSV must contain columns: {required}")

        df["date"] = df["date"].astype(str).apply(
            lambda x: dateparser.parse(x, settings={"DATE_ORDER": "YMD"}).date()
        )

        if df["date"].isna().any():
            bad_rows = df[df["date"].isna()]
            raise HTTPException(400, f"Incorrect Data Format: {bad_rows.index.tolist()}")

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