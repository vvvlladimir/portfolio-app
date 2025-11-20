from app.clients.yfinance_client import fetch_prices
from app.core.logger import logger
from app.repositories import RepositoryFactory


def refresh_all_tickers_data(db):
    """
    Fetches and updates price data for all tickers in the database.
    """
    factory = RepositoryFactory(db)

    ticker_repo = factory.get_ticker_repository()
    price_repo = factory.get_price_repository()

    tickers = [ticker.ticker for ticker in ticker_repo.get_tickers()]
    if not tickers:
        logger.warning("No tickers in database to refresh.")
        return 0

    try:
        price_data = fetch_prices(tickers)
        if price_data is not None:
            inserted = price_repo.upsert_bulk(price_data)
            logger.info(f"Updated {inserted} price records for {tickers}")
            return inserted
        else:
            logger.warning(f"No price data available for {tickers}")
            return 0
    except Exception as e:
        logger.error(f"Failed to refresh prices for {tickers}: {e}", exc_info=True)
        raise
