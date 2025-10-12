from typing import Iterable, List, Dict, Optional, Union
from datetime import date

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from app.models import Price
from app.repositories.base import BaseRepository, RepositoryError
import logging

logger = logging.getLogger(__name__)


class PriceRepository(BaseRepository[Price]):
    def __init__(self, db: Session):
        super().__init__(db, Price)

    def get_prices_by_filters(
            self,
            tickers: Optional[Iterable[str]] = None,
            date_from: Optional[date] = None,
            date_to: Optional[date] = None,
    ) -> List[Price]:
        """
        Get prices rows filtered by optional tickers and date range.
        """
        try:
            query = self.db.query(Price)

            if tickers:
                normalized_tickers = [t.upper() for t in tickers if t.strip()]
                if normalized_tickers:
                    query = query.filter(Price.ticker.in_(normalized_tickers))

            if date_from:
                query = query.filter(Price.date >= date_from)

            if date_to:
                query = query.filter(Price.date <= date_to)

            return query.order_by(Price.ticker, Price.date).all()

        except SQLAlchemyError as e:
            logger.error(f"Error getting prices with filters: {e}")
            raise RepositoryError("Failed to get prices") from e

    def get_latest_prices_by_ticker(self) -> Dict[str, Price]:
        """
        Get the latest price record for each ticker.
        """
        try:
            # Subquery to get max date for each ticker
            subquery = (
                self.db.query(
                    Price.ticker,
                    func.max(Price.date).label('max_date')
                )
                .group_by(Price.ticker)
                .subquery()
            )

            # Join to get full price records
            results = (
                self.db.query(Price)
                .join(
                    subquery,
                    (Price.ticker == subquery.c.ticker) &
                    (Price.date == subquery.c.max_date)
                )
                .all()
            )

            return {price.ticker: price for price in results}

        except SQLAlchemyError as e:
            logger.error(f"Error getting latest prices by ticker: {e}")
            raise RepositoryError("Failed to get latest prices") from e

    def get_last_dates_by_ticker(self) -> Dict[str, date]:
        """
        Dictionary of the last date with price for each ticker.
        """
        try:
            rows = (
                self.db.query(Price.ticker, func.max(Price.date))
                .group_by(Price.ticker)
                .all()
            )
            return {ticker: max_date for ticker, max_date in rows}

        except SQLAlchemyError as e:
            logger.error(f"Error getting last dates by ticker: {e}")
            raise RepositoryError("Failed to get last dates") from e

    def get_price_range(self, ticker: str, days: int = 30) -> List[Price]:
        """
        Get price history for a specific ticker for the last N days.
        """
        try:
            ticker = ticker.upper()
            return (
                self.db.query(Price)
                .filter(Price.ticker == ticker)
                .order_by(Price.date.desc())
                .limit(days)
                .all()
            )
        except SQLAlchemyError as e:
            logger.error(f"Error getting price range for {ticker}: {e}")
            raise RepositoryError(f"Failed to get price range for {ticker}") from e

    def upsert_bulk(self, data: Union[List[Dict], pd.DataFrame], **kwargs) -> int:
        """
        Bulk upsert price data with validation.
        """

        return super().upsert_bulk(
            data=data,
            index_elements=["ticker", "date"],
            validate_fn=self._validate_prices
        )

    def _validate_prices(self, rows: List[Dict]) -> List[Dict]:
        """Validate tickers are in DB before insertion."""
        try:
            from app.repositories import TickerRepository
            ticker_repo = TickerRepository(self.db)
            ticker_repo.upsert_bulk_missing(
                set({row.get("ticker", "") for row in rows if "ticker" in row})
            )
            return rows

        except Exception as e:
            logger.exception(f"Error validating tickers during price validation: {e}")
            return rows
