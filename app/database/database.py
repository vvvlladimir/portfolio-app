from sqlalchemy import Column, Integer, String, Numeric, Date, UniqueConstraint, create_engine, Computed, ForeignKey, BigInteger
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

from decouple import config

db_user = config("DB_USER", default="postgres")
db_password = config("DB_PASSWORD")
db_name = config("DB_NAME", default="postgres")

DATABASE_URL = f"postgresql://{db_user}:{db_password}@db:5432/{db_name}"
engine = create_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    """Dependency для получения database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, primary_key=True, nullable=False)
    type = Column(String, nullable=False)
    ticker = Column(String, ForeignKey("tickers.ticker"), nullable=False)
    currency = Column(String, nullable=False)
    shares = Column(Numeric(20, 8), nullable=False)
    value = Column(Numeric(20, 8), nullable=False)
    # TODO: add broker

    ticker_info = relationship("TickerInfo", back_populates="transactions")

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

class PortfolioHistory(Base):
    __tablename__ = "portfolio_history"

    date = Column(Date, primary_key=True, nullable=False)
    total_value = Column(Numeric(20, 8), nullable=False)
    invested_value = Column(Numeric(20, 8), nullable=False)
    gross_invested = Column(Numeric(20, 8), nullable=True)
    gross_withdrawn = Column(Numeric(20, 8), nullable=True)

    total_pnl = Column(Numeric(20, 8), Computed("total_value - invested_value"),)
    total_pnl_pct = Column(Numeric(20, 8), Computed(
        "CASE WHEN invested_value <> 0 "
        "THEN ((total_value - invested_value) / invested_value) * 100 "
        "ELSE 0 END"
    ))

class Position(Base):
    __tablename__ = "positions"

    date = Column(Date, primary_key=True)
    ticker = Column(String, ForeignKey("tickers.ticker"), primary_key=True)
    shares = Column(Numeric(20, 8), nullable=False)
    close = Column(Numeric(20, 8), nullable=True)
    gross_invested = Column(Numeric(20, 8), nullable=True)
    gross_withdraw = Column(Numeric(20, 8), nullable=True)
    total_pnl = Column(Numeric(20, 8), nullable=True)

    ticker_info = relationship("TickerInfo", back_populates="positions")

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


def init_db():
    Base.metadata.create_all(bind=engine)


if __name__ == "__main__":
    init_db()