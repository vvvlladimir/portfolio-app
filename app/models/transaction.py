from sqlalchemy import Column, Integer, String, Numeric, Date, ForeignKey
from sqlalchemy.orm import relationship
from app.core.db import Base

class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, primary_key=True, nullable=False)
    type = Column(String, nullable=False)
    ticker = Column(String, ForeignKey("tickers.ticker"), nullable=False)
    currency = Column(String, nullable=False)
    shares = Column(Numeric(20, 8), nullable=False)
    value = Column(Numeric(20, 8), nullable=False)

    ticker_info = relationship("TickerInfo", back_populates="transactions")