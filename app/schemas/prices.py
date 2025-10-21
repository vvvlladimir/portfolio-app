from pydantic import BaseModel, ConfigDict
from datetime import date

class PricesOut(BaseModel):
    ticker: str
    date: date
    high: float | None
    low: float | None
    open: float | None
    close: float | None
    volume: float | None

    model_config = ConfigDict(from_attributes=True)
