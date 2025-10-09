from typing import Iterable, List, Dict, Optional
from datetime import date
from sqlalchemy import func
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
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
                # Normalize tickers to uppercase
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

    def upsert_prices_bulk(self, rows: List[Dict]) -> int:
        """
        Bulk upsert prices with validation.
        Expected fields: ticker, date, open, high, low, close, volume.
        """
        if not rows:
            return 0

        try:
            # Validate and normalize data
            validated_rows = self._validate_price_rows(rows)

            stmt = insert(Price).values(validated_rows).on_conflict_do_nothing(
                index_elements=["ticker", "date"]
            )
            result = self.db.execute(stmt)
            self.db.commit()

            inserted_count = int(result.rowcount or 0)
            logger.info(f"Successfully upserted {inserted_count} price records")
            return inserted_count

        except Exception as e:
            self.db.rollback()
            logger.error(f"Error upserting prices: {e}")
            raise RepositoryError("Failed to upsert prices") from e

    def _validate_price_rows(self, rows: List[Dict]) -> List[Dict]:
        """
        Validate price data before insertion.
        """
        validated_rows = []
        required_fields = ['ticker', 'date', 'open', 'high', 'low', 'close']

        for i, row in enumerate(rows):
            # Check required fields
            missing_fields = [field for field in required_fields if field not in row or row[field] is None]
            if missing_fields:
                logger.warning(f"Row {i} missing required fields: {missing_fields}")
                continue

            # Normalize ticker
            validated_row = row.copy()
            validated_row['ticker'] = str(row['ticker']).upper().strip()

            # Validate price values
            price_fields = ['open', 'high', 'low', 'close']
            try:
                for field in price_fields:
                    value = float(row[field])
                    if value <= 0:
                        logger.warning(f"Row {i}: Invalid {field} value: {value}")
                        break
                    validated_row[field] = value
                else:
                    # Validate high >= low, etc.
                    if (validated_row['high'] >= validated_row['low'] and
                        validated_row['open'] > 0 and validated_row['close'] > 0):
                        validated_rows.append(validated_row)
                    else:
                        logger.warning(f"Row {i}: Invalid price relationships")
            except (ValueError, TypeError) as e:
                logger.warning(f"Row {i}: Invalid price data: {e}")
                continue

        return validated_rows

