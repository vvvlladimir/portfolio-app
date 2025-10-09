from sqlalchemy import Column, Date, Numeric, Computed
from app.core.db import Base

class PortfolioHistory(Base):
    __tablename__ = "portfolio_history"

    date = Column(Date, primary_key=True, nullable=False)
    total_value = Column(Numeric(20, 8), nullable=False)
    invested_value = Column(Numeric(20, 8), nullable=False)
    gross_invested = Column(Numeric(20, 8), nullable=True)
    gross_withdrawn = Column(Numeric(20, 8), nullable=True)

    total_pnl = Column(Numeric(20, 8), Computed("total_value - invested_value"))
    total_pnl_pct = Column(
        Numeric(20, 8),
        Computed(
            "CASE WHEN invested_value <> 0 "
            "THEN ((total_value - invested_value) / invested_value) * 100 "
            "ELSE 0 END"
        )
    )