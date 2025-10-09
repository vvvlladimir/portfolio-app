from pydantic import BaseModel, ConfigDict
from datetime import date

class PortfolioHistoryOut(BaseModel):
    date: date
    total_value: float | None
    invested_value: float | None
    gross_invested: float | None
    gross_withdrawn: float | None
    total_pnl: float | None
    total_pnl: float | None

    model_config = ConfigDict(from_attributes=True)

