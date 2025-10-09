from typing import List, Dict, Any, Optional
from datetime import date, datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func, desc
from app.models import PortfolioHistory
from app.repositories.base import BaseRepository, RepositoryError
import logging

logger = logging.getLogger(__name__)


class PortfolioHistoryRepository(BaseRepository[PortfolioHistory]):
    def __init__(self, db: Session):
        super().__init__(db, PortfolioHistory)

    def get_history_by_date_range(
        self,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None
    ) -> List[PortfolioHistory]:
        """
        Get portfolio history within a specific date range.
        """
        try:
            query = self.db.query(PortfolioHistory)

            if date_from:
                query = query.filter(PortfolioHistory.date >= date_from)
            if date_to:
                query = query.filter(PortfolioHistory.date <= date_to)

            return query.order_by(PortfolioHistory.date).all()

        except SQLAlchemyError as e:
            logger.error(f"Error getting portfolio history by date range: {e}")
            raise RepositoryError("Failed to get portfolio history") from e

    def get_latest_portfolio_value(self) -> Optional[PortfolioHistory]:
        """
        Get the most recent portfolio history record.
        """
        try:
            return (
                self.db.query(PortfolioHistory)
                .order_by(desc(PortfolioHistory.date))
                .first()
            )
        except SQLAlchemyError as e:
            logger.error(f"Error getting latest portfolio value: {e}")
            raise RepositoryError("Failed to get latest portfolio value") from e

    def get_portfolio_performance_summary(self) -> Dict[str, Any]:
        """
        Get portfolio performance summary statistics.
        """
        try:
            result = (
                self.db.query(
                    func.min(PortfolioHistory.total_value).label('min_value'),
                    func.max(PortfolioHistory.total_value).label('max_value'),
                    func.avg(PortfolioHistory.total_value).label('avg_value'),
                    func.min(PortfolioHistory.date).label('start_date'),
                    func.max(PortfolioHistory.date).label('end_date'),
                    func.count(PortfolioHistory.date).label('total_records')
                )
                .first()
            )

            if result:
                return {
                    'min_value': float(result.min_value) if result.min_value else 0,
                    'max_value': float(result.max_value) if result.max_value else 0,
                    'avg_value': float(result.avg_value) if result.avg_value else 0,
                    'start_date': result.start_date,
                    'end_date': result.end_date,
                    'total_records': int(result.total_records) if result.total_records else 0
                }
            return {}

        except SQLAlchemyError as e:
            logger.error(f"Error getting portfolio performance summary: {e}")
            raise RepositoryError("Failed to get portfolio performance summary") from e

    def delete_all_history(self) -> int:
        """
        Delete all records from the portfolio_history table.
        """
        try:
            deleted = self.db.query(PortfolioHistory).delete(synchronize_session=False)
            self.db.commit()
            logger.info(f"Deleted {deleted} portfolio history records")
            return int(deleted or 0)
        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"Error deleting all portfolio history: {e}")
            raise RepositoryError("Failed to delete portfolio history") from e

    def bulk_insert_history(self, rows: List[Dict[str, Any]]) -> int:
        """
        Bulk insert portfolio history records with validation.
        Expected fields: date, total_value, invested_value, gross_invested, gross_withdrawn
        """
        if not rows:
            return 0

        try:
            validated_rows = self._validate_history_rows(rows)

            self.db.bulk_insert_mappings(PortfolioHistory, validated_rows)
            self.db.commit()

            logger.info(f"Successfully inserted {len(validated_rows)} portfolio history records")
            return len(validated_rows)

        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"Error bulk inserting portfolio history: {e}")
            raise RepositoryError("Failed to bulk insert portfolio history") from e

    def _validate_history_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate portfolio history data before insertion.
        """
        validated_rows = []
        required_fields = ['date', 'total_value', 'invested_value']

        for i, row in enumerate(rows):
            # Check required fields
            missing_fields = [field for field in required_fields if field not in row or row[field] is None]
            if missing_fields:
                logger.warning(f"Row {i} missing required fields: {missing_fields}")
                continue

            validated_row = row.copy()

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
            numeric_fields = ['total_value', 'invested_value', 'gross_invested', 'gross_withdrawn']
            try:
                for field in numeric_fields:
                    if field in row and row[field] is not None:
                        validated_row[field] = float(row[field])
                        if field in ['total_value', 'invested_value'] and validated_row[field] < 0:
                            logger.warning(f"Row {i}: Negative value for {field}")
                            break
                else:
                    validated_rows.append(validated_row)
            except (ValueError, TypeError) as e:
                logger.warning(f"Row {i}: Invalid numeric data: {e}")
                continue

        return validated_rows


