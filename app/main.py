# app/main.py
from contextlib import asynccontextmanager
from sqlalchemy.orm import Session
from sqlalchemy import text
from fastapi import FastAPI, Request
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware
from app.api import api_router
from app.core.config import settings
from app.core.db import SessionLocal
from app.core.logger import logger
# from app.mcp_server import mcp
from app.scripts.init_db import init_db


def init_timescale(db: Session):
    try:
        db.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb;"))
        db.commit()
        logger.info("TimescaleDB extension ensured.")
    except Exception as e:
        logger.error(f"Error initializing TimescaleDB extension: {e}")
        db.rollback()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Starting up Portfolio API...")
    init_db()
    with SessionLocal() as db:
        init_timescale(db)
    logger.info("Database initialized and TimescaleDB ready.")
    yield
    logger.info("Shutting down Portfolio API.")


app = FastAPI(
    title="Portfolio API",
    version="1.0.0",
    debug=settings.DEBUG,
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True
)

@app.middleware("http")
async def add_ngrok_header(request: Request, call_next):
    response: Response = await call_next(request)
    response.headers["ngrok-skip-browser-warning"] = "true"
    return response

app.include_router(api_router)

# mcp_app = mcp.http_app(path="/mcp")
# app.mount("/mcp", mcp_app)
@app.get("/")
async def root():
    return {"message": "Portfolio API is running", "version": "1.0.0"}


if __name__ == "__main__":
    import uvicorn
    logger.info("Running API with Uvicorn...")
    uvicorn.run(app, host="0.0.0.0", port=8000)