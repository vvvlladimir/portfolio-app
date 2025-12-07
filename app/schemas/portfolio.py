from typing import List

from pydantic import BaseModel, ConfigDict
from datetime import date

class PortfolioHistoryOut(BaseModel):
    date: date
    total_value: float | None
    invested_value: float | None
    gross_invested: float | None
    gross_withdrawn: float | None
    total_pnl: float | None
    total_pnl_pct: float | None

    model_config = ConfigDict(from_attributes=True)

class PortfolioWeightsRow(BaseModel):
    date: date
    weights: List[float]

class PortfolioWeightsResponse(BaseModel):
    tickers: List[str]
    rows: List[PortfolioWeightsRow]

class PortfolioHistoryResponse(BaseModel):
    currency: str
    history: List[PortfolioHistoryOut]