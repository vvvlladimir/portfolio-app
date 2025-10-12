from typing import List, Dict, Any, Optional, Union
from datetime import date, datetime

import pandas as pd
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

    def upsert_bulk(self, data: Union[List[Dict], pd.DataFrame], **kwargs) -> int:
        """
        Bulk upsert price data with validation.
        """
        return super().upsert_bulk(
            data=data,
            index_elements=["date"],
        )

