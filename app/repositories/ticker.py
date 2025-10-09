from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func
from app.models import TickerInfo
from app.repositories.base import BaseRepository, RepositoryError
import logging

logger = logging.getLogger(__name__)


class TickerRepository(BaseRepository[TickerInfo]):
    def __init__(self, db: Session):
        super().__init__(db, TickerInfo)

    def get_tickers(self, **filters) -> List[TickerInfo]:
        """
        Get tickers filtered by optional criteria: symbol, exchange, currency, asset_type.
        """
        try:
            query = self.db.query(TickerInfo)
            mapping = {
                "symbol": TickerInfo.ticker,
                "exchange": TickerInfo.exchange,
                "currency": TickerInfo.currency,
                "asset_type": TickerInfo.asset_type,
            }

            for key, value in filters.items():
                if key in mapping and value:
                    query = query.filter(mapping[key] == self.normalize(value))

            return query.order_by(TickerInfo.ticker).all()

        except SQLAlchemyError as e:
            logger.error(f"Error fetching tickers with filters {filters}: {e}")
            raise RepositoryError(f"Failed to get tickers with filters {filters}") from e



    def get_ticker_summary(self) -> Dict[str, Any]:
        """
        Get ticker summary statistics.
        """
        try:
            result = (
                self.db.query(
                    func.count(TickerInfo.ticker).label('total_tickers'),
                    func.count(func.distinct(TickerInfo.exchange)).label('unique_exchanges'),
                    func.count(func.distinct(TickerInfo.currency)).label('unique_currencies'),
                    func.count(func.distinct(TickerInfo.asset_type)).label('unique_asset_types')
                )
                .first()
            )

            if result:
                return {
                    'total_tickers': int(result.total_tickers) if result.total_tickers else 0,
                    'unique_exchanges': int(result.unique_exchanges) if result.unique_exchanges else 0,
                    'unique_currencies': int(result.unique_currencies) if result.unique_currencies else 0,
                    'unique_asset_types': int(result.unique_asset_types) if result.unique_asset_types else 0
                }
            return {}

        except SQLAlchemyError as e:
            logger.error(f"Error getting ticker summary: {e}")
            raise RepositoryError("Failed to get ticker summary") from e

    def get_exchanges(self) -> List[str]:
        """
        Get all unique exchanges.
        """
        try:
            result = (
                self.db.query(TickerInfo.exchange)
                .distinct()
                .filter(TickerInfo.exchange.isnot(None))
                .order_by(TickerInfo.exchange)
                .all()
            )
            return [row.exchange for row in result]

        except SQLAlchemyError as e:
            logger.error(f"Error getting exchanges: {e}")
            raise RepositoryError("Failed to get exchanges") from e

    def get_currencies(self) -> List[str]:
        """
        Get all unique currencies.
        """
        try:
            result = (
                self.db.query(TickerInfo.currency)
                .distinct()
                .order_by(TickerInfo.currency)
                .all()
            )
            return [row.currency for row in result]

        except SQLAlchemyError as e:
            logger.error(f"Error getting currencies: {e}")
            raise RepositoryError("Failed to get currencies") from e

    def bulk_insert_tickers(self, rows: List[Dict[str, Any]]) -> int:
        """
        Bulk insert ticker records with validation.
        Expected fields: ticker, currency, long_name, exchange, asset_type
        """
        if not rows:
            return 0

        try:
            validated_rows = self._validate_ticker_rows(rows)

            self.db.bulk_insert_mappings(TickerInfo, validated_rows)
            self.db.commit()

            logger.info(f"Successfully inserted {len(validated_rows)} ticker records")
            return len(validated_rows)

        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"Error bulk inserting tickers: {e}")
            raise RepositoryError("Failed to bulk insert tickers") from e

    def _validate_ticker_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate ticker data before insertion.
        """
        validated_rows = []
        required_fields = ['ticker', 'currency']

        for i, row in enumerate(rows):
            # Check required fields
            missing_fields = [field for field in required_fields if field not in row or row[field] is None]
            if missing_fields:
                logger.warning(f"Row {i} missing required fields: {missing_fields}")
                continue

            validated_row = row.copy()

            # Normalize string fields
            validated_row['ticker'] = str(row['ticker']).upper().strip()
            validated_row['currency'] = str(row['currency']).upper().strip()

            if 'exchange' in row and row['exchange']:
                validated_row['exchange'] = str(row['exchange']).upper().strip()

            if 'asset_type' in row and row['asset_type']:
                validated_row['asset_type'] = str(row['asset_type']).upper().strip()

            # Validate ticker symbol is not empty
            if not validated_row['ticker']:
                logger.warning(f"Row {i}: Ticker symbol cannot be empty")
                continue

            validated_rows.append(validated_row)

        return validated_rows
