# app/mcp_server.py

from fastmcp import FastMCP
from sqlalchemy import func
from app.database.database import SessionLocal, Position
from app.services.portfolio_service import get_transactions
from app.database.upsert_data import upsert_positions, upsert_portfolio_history

mcp = FastMCP("Portfolio MCP")

@mcp.tool()
def last_positions():
    """Return the latest portfolio positions"""
    with SessionLocal() as db:
        last_date = db.query(func.max(Position.date)).scalar()
        rows = db.query(Position).filter(Position.date == last_date).all()
        positions = [
            {"ticker": r.ticker, "quantity": float(r.quantity), "price": float(r.price)}
            for r in rows
        ]
    return {
        "structuredContent": {"as_of": str(last_date), "positions": positions},
        "content": [{"type": "text", "text": f"Positions on {last_date} ({len(positions)})"}],
    }

@mcp.tool()
def transactions(limit: int | None = None):
    """Return the list of all transactions, optionally limited to a number of recent ones"""
    with SessionLocal() as db:
        items = get_transactions(db)
    if limit:
        items = items[:limit]
    return {
        "structuredContent": {"transactions": [i.model_dump() for i in items]},
        "content": [{"type": "text", "text": f"Transactions: {len(items)}"}],
    }

@mcp.tool()
def refresh_portfolio_history():
    """Recompute the entire portfolio history from transactions and prices"""
    with SessionLocal() as db:
        upsert_positions(db)
        data = upsert_portfolio_history(db)
    return {
        "structuredContent": {"status": "ok", "recomputed": True, "rows": len(data or [])},
        "content": [{"type": "text", "text": "Portfolio history recomputed"}],
    }

