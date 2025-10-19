from typing import List, Dict, Any, Optional, Union
from datetime import date, datetime

import pandas as pd
from sqlalchemy.orm import joinedload, Session, noload
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func, desc
from app.models import Transaction
from app.repositories.base import BaseRepository, RepositoryError
import logging

logger = logging.getLogger(__name__)


class TransactionRepository(BaseRepository[Transaction]):
    def __init__(self, db: Session):
        super().__init__(db, Transaction)

    def get_all_tickers(self) -> List[str]:
        """
        Get all unique tickers from transactions.
        """
        try:
            result = (
                self.db.query(Transaction.ticker)
                .distinct()
                .order_by(Transaction.ticker)
                .all()
            )
            return [row.ticker for row in result]

        except SQLAlchemyError as e:
            logger.error(f"Error getting all tickers: {e}")
            raise RepositoryError("Failed to get all tickers") from e


    def get_by_filters(
            self,
            ticker: Optional[str] = None,
            type: Optional[str] = None,
            date_from: Optional[date] = None,
            date_to: Optional[date] = None,
            include_ticker_info: bool = False,
            **kwargs
    ) -> List[Transaction]:
        """
        Override base get_by_filters to handle Transaction-specific filtering logic.
        """
        try:
            query = self.db.query(Transaction)

            if include_ticker_info:
                query = query.options(joinedload(Transaction.ticker_info))
            else:
                query = query.options(noload(Transaction.ticker_info))

            if date_from:
                query = query.filter(Transaction.date >= date_from)

            if date_to:
                query = query.filter(Transaction.date <= date_to)


            for key, value in kwargs.items():
                if hasattr(Transaction, key) and value is not None:
                    query = query.filter(getattr(Transaction, key) == value)

            return query.order_by(Transaction.date.desc()).all()

        except SQLAlchemyError as e:
            logger.error(f"Error filtering transactions: {e}")
            raise RepositoryError("Failed to filter transactions") from e


    def get_transaction_types(self) -> List[str]:
        """
        Get all unique transaction types in the database.
        """
        try:
            result = (
                self.db.query(Transaction.type)
                .distinct()
                .order_by(Transaction.type)
                .all()
            )
            return [row.type for row in result]

        except SQLAlchemyError as e:
            logger.error(f"Error getting transaction types: {e}")
            raise RepositoryError("Failed to get transaction types") from e

    def get_transaction_currencies(self) -> List[str]:
        """
        Get all unique currencies types in the database.
        """
        try:
            result = (
                self.db.query(Transaction.currency)
                .distinct()
                .order_by(Transaction.currency)
                .all()
            )
            return [row.currency for row in result]

        except SQLAlchemyError as e:
            logger.error(f"Error getting transaction currencies: {e}")
            raise RepositoryError("Failed to get transaction currencies") from e

    def upsert_bulk(self, data: Union[List[Dict], pd.DataFrame], **kwargs) -> int:
        """
        Bulk upsert price data with validation.
        """

        return super().upsert_bulk(
            data=data,
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