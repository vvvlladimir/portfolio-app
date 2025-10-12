from typing import List, Dict, Any, Optional, Union
from datetime import date, datetime

import pandas as pd
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


    def upsert_bulk(self, data: Union[List[Dict], pd.DataFrame], **kwargs) -> int:
        """
        Bulk upsert price data with validation.
        """

        return super().upsert_bulk(
            data=data,
            index_elements=["date", "ticker"]
        )

