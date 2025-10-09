# app/main.py
from contextlib import asynccontextmanager
from sqlalchemy.orm import Session
from sqlalchemy import text

from fastapi import FastAPI, Request
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.database.database import init_db, SessionLocal
from app.mcp_server import mcp
from app.routers import upload, ticker, portfolio

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

@app.middleware("http")
async def add_ngrok_header(request: Request, call_next):
    response: Response = await call_next(request)
    response.headers["ngrok-skip-browser-warning"] = "true"
    return response

app.include_router(upload.router)
app.include_router(ticker.router)
app.include_router(portfolio.router)

mcp_app = mcp.http_app(path='/mcp')

# Key: Pass lifespan to FastAPI
app = FastAPI(title="Portfolio MCP", lifespan=mcp_app.lifespan)
app.mount("/portfolio", mcp_app)
@app.get("/")
async def root():
    return {"message": "Portfolio API is running", "version": "1.0.0"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)