from typing import Optional, List, Dict
from pydantic import BaseModel, ConfigDict
from datetime import date

class PositionsOut(BaseModel):
    date: date
    ticker: str | None
    shares: float | None
    close: float | None
    gross_invested: float | None
    gross_withdrawn: float | None
    cum_invested: float | None = None
    cum_withdrawn: float | None = None
    total_pnl: float | None

    model_config = ConfigDict(from_attributes=True)

class PeriodStat(BaseModel):
    start_date: date
    end_date: date
    twr_pct: Optional[float] = None
    pnl_abs: float
    cash_in: float
    cash_out: float
    mv_start: float
    mv_end: float

    model_config = ConfigDict(from_attributes=True)


class PositionsStatsOut(BaseModel):
    ticker: str
    as_of: date
    currency: Optional[str] = None
    market_value: float
    total_pnl: float
    total_pnl_pct: Optional[float] = None
    cum_invested: float
    cum_withdrawn: float
    periods: Dict[str, Optional[PeriodStat]]

    model_config = ConfigDict(from_attributes=True)