from sqlalchemy import Column, String
from sqlalchemy.orm import relationship
from app.core.db import Base

class TickerInfo(Base):
    __tablename__ = "tickers"

    ticker = Column(String, primary_key=True, unique=True, nullable=False)
    currency = Column(String, nullable=False)
    long_name = Column(String, nullable=True)
    exchange = Column(String, nullable=True)
    asset_type = Column(String, nullable=True)

    prices = relationship("Price", back_populates="ticker_info")
    positions = relationship("Position", back_populates="ticker_info")
    transactions = relationship("Transaction", back_populates="ticker_info")