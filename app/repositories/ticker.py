from typing import List, Dict, Any, Union

import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func

from app.clients.yfinance_client import fetch_ticker_info
from app.models import TickerInfo
from app.repositories.base import BaseRepository, RepositoryError
import logging

logger = logging.getLogger(__name__)


class TickerRepository(BaseRepository[TickerInfo]):
    def __init__(self, db: Session):
        super().__init__(db, TickerInfo)

    def get_tickers(self, **filters) -> List[TickerInfo]:
        """Get tickers filtered by optional criteria: symbol, exchange, currency, asset_type"""
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
                    # TODO: fix normalization
                    query = query.filter(mapping[key] == self.normalize_value(value))
            return query.order_by(TickerInfo.ticker).all()

        except SQLAlchemyError as e:
            logger.error(f"Error fetching tickers with filters {filters}: {e}")
            raise RepositoryError(f"Failed to get tickers with filters {filters}") from e



    def get_ticker_summary(self) -> Dict[str, Any]:
        """Get ticker summary statistics"""
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

    def upsert_bulk_missing(self, tickers: List[str]) -> int:
        """
        Update missing ticker info records.
        """
        try:
            existing_tickers = {ticker.ticker for ticker in self.get_tickers()}
            missing_tickers = set(tickers) - existing_tickers
            if missing_tickers:
                tickers_data = [fetch_ticker_info(ticker) for ticker in missing_tickers]
                return self.upsert_bulk(tickers_data)
            return 0
        except Exception as e:
            logger.error(f"Error updating missing tickers: {tickers}")
            raise RepositoryError(f"Failed to update missing tickers: {e}")

    def upsert_bulk(self, data: Union[List[Dict], pd.DataFrame], **kwargs) -> int:
        """
        Bulk upsert price data.
        """
        return super().upsert_bulk(
            data=data,
            index_elements=["ticker"]
        )
