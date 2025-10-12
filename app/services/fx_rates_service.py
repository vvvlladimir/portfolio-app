from itertools import combinations
from typing import Optional, Set, Tuple, List
import pandas as pd

from app.clients.yfinance_client import fetch_prices
from app.core.logger import logger


class FXRateService:
    """Service for handling FX rates and currency conversions."""

    def __init__(self, factory, fetch_prices_fn=fetch_prices):
        """
        :param factory: RepositoryFactory
        :param fetch_prices_fn: function to fetch prices, e.g. from yfinance
        """
        self.factory = factory
        self.fetch_prices = fetch_prices_fn
        self.price_repo = factory.get_price_repository()

    def get_fx_rates(
            self,
            source_currencies: Set[str],
            df_prices: pd.DataFrame,
            target_currency: Optional[str] = None,
            start_date: Optional[str | pd.Timestamp] = None,
    ) -> pd.DataFrame:
        """
        Utility to get FX rates for given source currencies.
        Returns DF ['fx_ticker', 'date', 'rate']
        """
        df_prices["date"] = pd.to_datetime(df_prices["date"])
        if start_date is not None:
            df_prices = df_prices[df_prices["date"] >= pd.to_datetime(start_date)]
        existing_fx = set(df_prices["ticker"].unique())

        fx_pairs, missing_fx = self.get_needed_pairs(
            source_currencies=source_currencies,
            target_currency=target_currency,
            existing_fx=existing_fx,
        )

        if missing_fx:
            df_prices = self._update_prices_with_missing(df_prices, missing_fx)

        df_fx = (
            df_prices[df_prices["ticker"].isin(fx_pairs)]
            [["ticker", "date", "close"]]
            .rename(columns={"ticker": "fx_ticker", "close": "rate"})
        )
        df_fx["date"] = pd.to_datetime(df_fx["date"])
        return df_fx

    def get_needed_pairs(
            self,
            source_currencies: Set[str],
            target_currency: Optional[str] = None,
            existing_fx: Optional [Set[str]] = None,
    ) -> Tuple[List[str], List[str]]:
        """Determines which FX pairs are needed and which are missing."""

        def pair(base: str, quote: str) -> str:
            return f"{base}{quote}=X"

        fx_pairs, missing_fx = [], []

        if not existing_fx:
            if target_currency:
                for curr in source_currencies:
                    if curr != target_currency:
                        missing_fx.extend([pair(curr, target_currency), pair(target_currency, curr)])
            else:
                for base, quote in combinations(source_currencies, 2):
                    missing_fx.extend([pair(base, quote), pair(quote, base)])
            return [], missing_fx

        if target_currency:
            for curr in source_currencies:
                if curr == target_currency:
                    continue
                direct, inverse = pair(curr, target_currency), pair(target_currency, curr)
                if direct in existing_fx:
                    fx_pairs.append(direct)
                elif inverse in existing_fx:
                    fx_pairs.append(inverse)
                else:
                    missing_fx.extend([direct, inverse])
        else:
            for base, quote in combinations(source_currencies, 2):
                direct, inverse = pair(base, quote), pair(quote, base)
                if direct in existing_fx:
                    fx_pairs.append(direct)
                elif inverse in existing_fx:
                    fx_pairs.append(inverse)
                else:
                    missing_fx.extend([direct, inverse])

        return fx_pairs, missing_fx

    def _update_prices_with_missing(
            self, df_prices: pd.DataFrame, missing_fx: List[str]
    ) -> pd.DataFrame:
        """Downloads and appends missing FX prices."""
        logger.info(f"Fetching {len(missing_fx)} missing FX pairs: {missing_fx}")
        rows = self.fetch_prices(missing_fx)
        if rows is not None and len(rows):
            self.price_repo.upsert_bulk(rows)
            df_prices = pd.concat([df_prices, pd.DataFrame(rows)], ignore_index=True)
        return df_prices