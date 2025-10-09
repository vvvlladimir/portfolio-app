from fastapi import APIRouter

from .routes.prices import router as price_router
from .routes.transactions import router as transactions_router
from .routes.positions import router as positions_router
from .routes.portfolio import router as portfolio_router
from .routes.tickers import router as tickers_router
from .routes.upload import router as upload_router

api_router = APIRouter()
api_router.include_router(price_router, prefix="/prices", tags=["Prices"])
api_router.include_router(transactions_router, prefix="/transactions", tags=["Transactions"])
api_router.include_router(positions_router, prefix="/positions", tags=["Positions"])
api_router.include_router(portfolio_router, prefix="/portfolio", tags=["Portfolio"])
api_router.include_router(tickers_router, prefix="/tickers", tags=["Tickers"])
api_router.include_router(upload_router, prefix="/upload", tags=["Upload"])