from locale import currency

from fastapi import APIRouter, UploadFile, File, Depends
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
from app.database import Transaction, get_db, TickerInfo
from app.services.ticker_service import fetch_ticker_info, upsert_ticker_info, upsert_missing_tickers_info

router = APIRouter(prefix="/upload", tags=["upload"])

@router.post("/csv")
async def upload_csv(file: UploadFile = File(...), db: Session = Depends(get_db)):
    try:
        df = pd.read_csv(file.file)
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]

        df["date"] = df["date"].str.replace(r"\s*\(.*\)$", "", regex=True)
        df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)

        type_map = {
            "buy": "BUY",
            "purchase": "BUY",
            "sell": "SELL",
            "revenue": "SELL",
            "expenses": "BUY"
        }

        inserted = 0
        upsert_missing_tickers_info(db, df["ticker"].unique().tolist())

        for _, row in df.iterrows():
            raw_type = str(row["type"]).strip().lower()
            mapped_type = type_map.get(raw_type)
            if mapped_type is None:
                continue

            stmt = insert(Transaction).values(
                date=row["date"],
                type=mapped_type,
                ticker=str(row["ticker"]).upper(),
                currency=str("USD"),
                shares=float(row["shares"]),
                value=float(row["value"]),
                filled_price=float(row["filled_price"]),
            ).on_conflict_do_nothing()

            result = db.execute(stmt)
            if result.rowcount > 0:
                inserted += 1

        db.commit()
        return {"status": "ok", "rows_inserted": inserted}
    except Exception as e:
        db.rollback()
        return {"status": "error", "message": str(e)}
