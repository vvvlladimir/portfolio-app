from fastapi import FastAPI
from sqlalchemy import text
from contextlib import asynccontextmanager
from sqlalchemy.orm import Session
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.database.database import init_db, SessionLocal
from app.routers import upload, ticker, portfolio

"""
# Tune TimescaleDB settings on prod:

    ALTER SYSTEM SET timescaledb.enable_cagg_window_functions = on;
    SELECT pg_reload_conf();
"""


def init_timescale(db: Session):
    db.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb;"))
    db.commit()



@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    with SessionLocal() as db:
        init_timescale(db)
    print("Database initialized")
    yield
    print("Database connection closed")


app = FastAPI(lifespan=lifespan, title="Portfolio API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True
)

app.include_router(upload.router)
app.include_router(ticker.router)
app.include_router(portfolio.router)


@app.get("/")
async def root():
    return {"message": "Portfolio API is running", "version": "1.0.0"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0", port=8000)