from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict
from datetime import date

from app.schemas.ticker import TickerOut


class TransactionType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    DEPOSIT = "DEPOSIT"
    WITHDRAW = "WITHDRAW"

    @classmethod
    def inflows(cls):
        return [cls.BUY, cls.DEPOSIT]

    @classmethod
    def outflows(cls):
        return [cls.WITHDRAW, cls.SELL]

class TransactionsOut(BaseModel):
    id: int
    date: date
    type: str | None
    ticker: str | None
    currency: str | None
    shares: float | None
    value: float | None

    model_config = ConfigDict(from_attributes=True)