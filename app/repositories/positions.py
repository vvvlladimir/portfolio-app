from typing import List, Dict, Any, Optional
from datetime import date, datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func, desc
from app.models import Position
from app.repositories.base import BaseRepository, RepositoryError
import logging

logger = logging.getLogger(__name__)


class PositionRepository(BaseRepository[Position]):
    def __init__(self, db: Session):
        super().__init__(db, Position)

    def get_positions_by_date_range(
        self,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        ticker: Optional[str] = None
    ) -> List[Position]:
        """
        Get positions within a specific date range, optionally filtered by ticker.
        """
        try:
            query = self.db.query(Position)

            if date_from:
                query = query.filter(Position.date >= date_from)
            if date_to:
                query = query.filter(Position.date <= date_to)
            if ticker:
                query = query.filter(Position.ticker == ticker.upper())

            return query.order_by(Position.date, Position.ticker).all()

        except SQLAlchemyError as e:
            logger.error(f"Error getting positions by date range: {e}")
            raise RepositoryError("Failed to get positions") from e

    def get_latest_positions(self) -> List[Position]:
        """
        Get the most recent positions for all tickers.
        """
        try:
            # Subquery to get max date for each ticker
            subquery = (
                self.db.query(
                    Position.ticker,
                    func.max(Position.date).label('max_date')
                )
                .group_by(Position.ticker)
                .subquery()
            )

            # Join to get full position records
            return (
                self.db.query(Position)
                .join(
                    subquery,
                    (Position.ticker == subquery.c.ticker) &
                    (Position.date == subquery.c.max_date)
                )
                .order_by(Position.ticker)
                .all()
            )

        except SQLAlchemyError as e:
            logger.error(f"Error getting latest positions: {e}")
            raise RepositoryError("Failed to get latest positions") from e

    def get_position_history(self, ticker: str, days: int = 30) -> List[Position]:
        """
        Get position history for a specific ticker for the last N days.
        """
        try:
            ticker = ticker.upper()
            return (
                self.db.query(Position)
                .filter(Position.ticker == ticker)
                .order_by(desc(Position.date))
                .limit(days)
                .all()
            )
        except SQLAlchemyError as e:
            logger.error(f"Error getting position history for {ticker}: {e}")
            raise RepositoryError(f"Failed to get position history for {ticker}") from e

    def get_positions_summary(self) -> Dict[str, Any]:
        """
        Get positions summary statistics.
        """
        try:
            result = (
                self.db.query(
                    func.count(func.distinct(Position.ticker)).label('unique_tickers'),
                    func.sum(Position.shares).label('total_shares'),
                    func.sum(Position.gross_invested).label('total_invested'),
                    func.sum(Position.gross_withdrawn).label('total_withdrawn'),
                    func.sum(Position.total_pnl).label('total_pnl'),
                    func.min(Position.date).label('start_date'),
                    func.max(Position.date).label('end_date')
                )
                .first()
            )

            if result:
                return {
                    'unique_tickers': int(result.unique_tickers) if result.unique_tickers else 0,
                    'total_shares': float(result.total_shares) if result.total_shares else 0,
                    'total_invested': float(result.total_invested) if result.total_invested else 0,
                    'total_withdrawn': float(result.total_withdrawn) if result.total_withdrawn else 0,
                    'total_pnl': float(result.total_pnl) if result.total_pnl else 0,
                    'start_date': result.start_date,
                    'end_date': result.end_date
                }
            return {}

        except SQLAlchemyError as e:
            logger.error(f"Error getting positions summary: {e}")
            raise RepositoryError("Failed to get positions summary") from e

    def delete_all_positions(self) -> int:
        """
        Delete all records from the positions table.
        """
        try:
            deleted = self.db.query(Position).delete(synchronize_session=False)
            self.db.commit()
            logger.info(f"Deleted {deleted} position records")
            return int(deleted or 0)
        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"Error deleting all positions: {e}")
            raise RepositoryError("Failed to delete positions") from e

    def bulk_insert_positions(self, rows: List[Dict[str, Any]]) -> int:
        """
        Bulk insert position records with validation.
        Expected fields: date, ticker, shares, close, gross_invested, gross_withdrawn, total_pnl
        """
        if not rows:
            return 0

        try:
            validated_rows = self._validate_position_rows(rows)

            self.db.bulk_insert_mappings(Position, validated_rows)
            self.db.commit()

            logger.info(f"Successfully inserted {len(validated_rows)} position records")
            return len(validated_rows)

        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"Error bulk inserting positions: {e}")
            raise RepositoryError("Failed to bulk insert positions") from e

    def _validate_position_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate position data before insertion.
        """
        validated_rows = []
        required_fields = ['date', 'ticker', 'shares']

        for i, row in enumerate(rows):
            # Check required fields
            missing_fields = [field for field in required_fields if field not in row or row[field] is None]
            if missing_fields:
                logger.warning(f"Row {i} missing required fields: {missing_fields}")
                continue

            validated_row = row.copy()

            # Normalize ticker
            validated_row['ticker'] = str(row['ticker']).upper().strip()

            # Validate date
            try:
                if isinstance(row['date'], str):
                    validated_row['date'] = datetime.strptime(row['date'], '%Y-%m-%d').date()
                elif isinstance(row['date'], datetime):
                    validated_row['date'] = row['date'].date()
            except (ValueError, TypeError) as e:
                logger.warning(f"Row {i}: Invalid date format: {e}")
                continue

            # Validate numeric values
            numeric_fields = ['shares', 'close', 'gross_invested', 'gross_withdrawn', 'total_pnl']
            try:
                for field in numeric_fields:
                    if field in row and row[field] is not None:
                        validated_row[field] = float(row[field])

                # Validate shares is not zero
                if validated_row.get('shares', 0) == 0:
                    logger.warning(f"Row {i}: Shares cannot be zero")
                    continue

                validated_rows.append(validated_row)

            except (ValueError, TypeError) as e:
                logger.warning(f"Row {i}: Invalid numeric data: {e}")
                continue

        return validated_rows

