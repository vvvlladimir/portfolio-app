from pydantic import BaseModel, ConfigDict
from typing import List

class TickersRequest(BaseModel):
    tickers: List[str]

class TickerOut(BaseModel):
    ticker: str
    currency: str
    long_name: str | None
    exchange: str | None
    asset_type: str | None

    model_config = ConfigDict(from_attributes=True)
