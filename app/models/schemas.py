from pydantic import BaseModel, ConfigDict
from typing import List
from datetime import date

class TickersRequest(BaseModel):
    tickers: List[str]

class TickerOut(BaseModel):
    ticker: str
    currency: str
    long_name: str | None
    exchange: str | None
    asset_type: str | None

    model_config = ConfigDict(from_attributes=True)


class PortfolioHistoryOut(BaseModel):
    date: date
    total_value: float | None
    invested_value: float | None
    gross_invested: float | None
    gross_withdrawn: float | None
    total_pnl: float | None
    total_pnl: float | None

    model_config = ConfigDict(from_attributes=True)

class TransactionsOut(BaseModel):
    id: int
    date: date
    type: str | None
    ticker: str | None
    currency: str | None
    shares: float | None
    value: float | None
    ticker_info: TickerOut

    model_config = ConfigDict(from_attributes=True)

class PositionsOut(BaseModel):
    date: date
    ticker: str | None
    shares: float | None
    close: float | None
    gross_invested: float | None
    gross_withdraw: float | None
    total_pnl: float | None
    ticker_info: TickerOut

    model_config = ConfigDict(from_attributes=True)