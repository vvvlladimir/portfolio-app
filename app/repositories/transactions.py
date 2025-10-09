from typing import List, Dict, Any, Optional
from datetime import date, datetime
from sqlalchemy.orm import joinedload, Session
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


    def get_transactions_by_filters(
        self,
        ticker: Optional[str] = None,
        transaction_type: Optional[str] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        include_ticker_info: bool = True
    ) -> List[Transaction]:
        """
        Get transactions filtered by optional criteria.
        """
        try:
            query = self.db.query(Transaction)

            if include_ticker_info:
                query = query.options(joinedload(Transaction.ticker_info))

            if ticker:
                query = query.filter(Transaction.ticker == ticker.upper())

            if transaction_type:
                query = query.filter(Transaction.type == transaction_type.upper())

            if date_from:
                query = query.filter(Transaction.date >= date_from)

            if date_to:
                query = query.filter(Transaction.date <= date_to)

            return query.order_by(Transaction.date.desc()).all()

        except SQLAlchemyError as e:
            logger.error(f"Error getting transactions with filters: {e}")
            raise RepositoryError("Failed to get transactions") from e

    def get_transactions_summary(self) -> Dict[str, Any]:
        """
        Get transactions summary statistics.
        """
        try:
            result = (
                self.db.query(
                    func.count(Transaction.id).label('total_transactions'),
                    func.count(func.distinct(Transaction.ticker)).label('unique_tickers'),
                    func.sum(func.case(
                        [(Transaction.type == 'BUY', Transaction.value)], else_=0
                    )).label('total_bought'),
                    func.sum(func.case(
                        [(Transaction.type == 'SELL', Transaction.value)], else_=0
                    )).label('total_sold'),
                    func.sum(func.case(
                        [(Transaction.type == 'BUY', Transaction.shares)], else_=0
                    )).label('shares_bought'),
                    func.sum(func.case(
                        [(Transaction.type == 'SELL', Transaction.shares)], else_=0
                    )).label('shares_sold'),
                    func.min(Transaction.date).label('first_transaction_date'),
                    func.max(Transaction.date).label('last_transaction_date')
                )
                .first()
            )

            if result:
                return {
                    'total_transactions': int(result.total_transactions) if result.total_transactions else 0,
                    'unique_tickers': int(result.unique_tickers) if result.unique_tickers else 0,
                    'total_bought': float(result.total_bought) if result.total_bought else 0,
                    'total_sold': float(result.total_sold) if result.total_sold else 0,
                    'shares_bought': float(result.shares_bought) if result.shares_bought else 0,
                    'shares_sold': float(result.shares_sold) if result.shares_sold else 0,
                    'net_investment': float((result.total_bought or 0) - (result.total_sold or 0)),
                    'first_transaction_date': result.first_transaction_date,
                    'last_transaction_date': result.last_transaction_date
                }
            return {}

        except SQLAlchemyError as e:
            logger.error(f"Error getting transactions summary: {e}")
            raise RepositoryError("Failed to get transactions summary") from e

    def get_transactions_by_ticker(self, ticker: str, include_ticker_info: bool = True) -> List[Transaction]:
        """
        Get all transactions for a specific ticker.
        """
        try:
            ticker = ticker.upper()
            query = self.db.query(Transaction).filter(Transaction.ticker == ticker)

            if include_ticker_info:
                query = query.options(joinedload(Transaction.ticker_info))

            return query.order_by(Transaction.date.desc()).all()

        except SQLAlchemyError as e:
            logger.error(f"Error getting transactions for ticker {ticker}: {e}")
            raise RepositoryError(f"Failed to get transactions for ticker {ticker}") from e

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

    def bulk_insert_transactions(self, rows: List[Dict[str, Any]]) -> int:
        """
        Bulk insert transaction records with validation.
        Expected fields: date, type, ticker, currency, shares, value
        """
        if not rows:
            return 0

        try:
            validated_rows = self._validate_transaction_rows(rows)

            self.db.bulk_insert_mappings(Transaction, validated_rows)
            self.db.commit()

            logger.info(f"Successfully inserted {len(validated_rows)} transaction records")
            return len(validated_rows)

        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"Error bulk inserting transactions: {e}")
            raise RepositoryError("Failed to bulk insert transactions") from e

    def _validate_transaction_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate transaction data before insertion.
        """
        validated_rows = []
        required_fields = ['date', 'type', 'ticker', 'currency', 'shares', 'value']
        valid_types = ['BUY', 'SELL', 'DIVIDEND']

        for i, row in enumerate(rows):
            # Check required fields
            missing_fields = [field for field in required_fields if field not in row or row[field] is None]
            if missing_fields:
                logger.warning(f"Row {i} missing required fields: {missing_fields}")
                continue

            validated_row = row.copy()

            # Normalize ticker and type
            validated_row['ticker'] = str(row['ticker']).upper().strip()
            validated_row['type'] = str(row['type']).upper().strip()
            validated_row['currency'] = str(row['currency']).upper().strip()

            # Validate transaction type
            if validated_row['type'] not in valid_types:
                logger.warning(f"Row {i}: Invalid transaction type: {validated_row['type']}")
                continue

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
            try:
                validated_row['shares'] = float(row['shares'])
                validated_row['value'] = float(row['value'])

                if validated_row['shares'] <= 0:
                    logger.warning(f"Row {i}: Shares must be positive")
                    continue

                if validated_row['value'] <= 0:
                    logger.warning(f"Row {i}: Value must be positive")
                    continue

                validated_rows.append(validated_row)

            except (ValueError, TypeError) as e:
                logger.warning(f"Row {i}: Invalid numeric data: {e}")
                continue

        return validated_rows