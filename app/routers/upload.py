import dateparser
import pandas as pd
from app.database.database import Transaction, get_db
from app.database.upsert_data import upsert_missing_tickers_info, upsert_all_prices, upsert_positions, \
    upsert_portfolio_history
from app.services.portfolio_service import get_transactions
from fastapi import APIRouter, UploadFile, File, Depends
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

router = APIRouter(prefix="/upload", tags=["upload"])

@router.post("/csv")
async def upload_csv(
        file: UploadFile = File(...),
        db: Session = Depends(get_db),
):
    try:
        df = pd.read_csv(file.file)
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        df = df.dropna()

        df["date"] = df["date"].astype(str).apply(
            lambda x: dateparser.parse(x, settings={"DATE_ORDER": "YMD"}).date()
        )

        if df["date"].isna().any():
            bad_rows = df[df["date"].isna()]
            raise ValueError(f"Incorrect Data Format: {bad_rows.index.tolist()}")

        upsert_missing_tickers_info(db, df["ticker"].unique().tolist())

        for _, row in df.iterrows():
            stmt = insert(Transaction).values(
                date=row["date"],
                type=str(row["type"]).strip().upper(),
                ticker=str(row["ticker"]).upper(),
                currency=str(row["currency"]).upper(),
                shares=float(row["shares"]),
                value=float(row["value"]),
            ).on_conflict_do_nothing()

            db.execute(stmt)
        db.commit()

        upsert_all_prices(db)
        upsert_positions(db)
        upsert_portfolio_history(db)
        return get_transactions(db)

    except Exception as e:
        db.rollback()
        return {"status": "error", "message": str(e)}