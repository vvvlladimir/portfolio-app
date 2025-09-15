from fastapi import FastAPI
from sqlalchemy import text
from contextlib import asynccontextmanager
from sqlalchemy.orm import Session
from fastapi.middleware.cors import CORSMiddleware

from app.database import SessionLocal, init_db
from app.config import settings
from app.routers import upload, ticker, portfolio


def init_timescale(db: Session):
    db.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb;"))
    """Инициализация TimescaleDB hypertables"""
    db.execute(text("""
        SELECT create_hypertable('prices', 'date', if_not_exists => TRUE);
    """))
    db.execute(text("""
        SELECT create_hypertable('portfolio_history', 'date', if_not_exists => TRUE);
    """))
    db.commit()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle события приложения"""
    init_db()
    with SessionLocal() as db:
        init_timescale(db)
    print("Database initialized")
    yield
    print("Database connection closed")


# Создание приложения
app = FastAPI(lifespan=lifespan, title="Portfolio API", version="1.0.0")

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True
)

# Подключение роутеров
app.include_router(upload.router)
app.include_router(ticker.router)
app.include_router(portfolio.router)


@app.get("/")
async def root():
    """Корневой эндпоинт"""
    return {"message": "Portfolio API is running", "version": "1.0.0"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0", port=8000)