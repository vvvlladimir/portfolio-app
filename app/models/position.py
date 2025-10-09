from sqlalchemy import Column, String, Date, Numeric, ForeignKey
from sqlalchemy.orm import relationship
from app.core.db import Base

class Position(Base):
    __tablename__ = "positions"

    date = Column(Date, primary_key=True)
    ticker = Column(String, ForeignKey("tickers.ticker"), primary_key=True)
    shares = Column(Numeric(20, 8), nullable=False)
    close = Column(Numeric(20, 8), nullable=True)
    gross_invested = Column(Numeric(20, 8), nullable=True)
    gross_withdrawn = Column(Numeric(20, 8), nullable=True)
    total_pnl = Column(Numeric(20, 8), nullable=True)

    ticker_info = relationship("TickerInfo", back_populates="positions")