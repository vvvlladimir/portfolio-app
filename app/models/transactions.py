from enum import Enum


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