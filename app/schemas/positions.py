from typing import Optional

from pydantic import BaseModel, ConfigDict
from datetime import date
from app.schemas.ticker import TickerOut


class PositionsOut(BaseModel):
    date: date
    ticker: str | None
    shares: float | None
    close: float | None
    gross_invested: float | None
    gross_withdrawn: float | None
    total_pnl: float | None
    ticker_info: Optional[TickerOut] = None

    model_config = ConfigDict(from_attributes=True)