from sqlalchemy import Column, String, Date, Numeric, BigInteger, ForeignKey
from sqlalchemy.orm import relationship
from app.core.db import Base

class Price(Base):
    __tablename__ = "prices"

    ticker = Column(String, ForeignKey("tickers.ticker"), primary_key=True)
    date = Column(Date, primary_key=True)
    open = Column(Numeric(20, 8), nullable=False)
    high = Column(Numeric(20, 8), nullable=False)
    low = Column(Numeric(20, 8), nullable=False)
    close = Column(Numeric(20, 8), nullable=False)
    volume = Column(BigInteger, nullable=True)

    ticker_info = relationship("TickerInfo", back_populates="prices")