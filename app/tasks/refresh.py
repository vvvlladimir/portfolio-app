from app.core.celery_app import celery
from app.core.logger import logger
from app.managers.cache_manager import CacheManager
from app.services.market_data import refresh_all_tickers_data
from app.core.db import SessionLocal

@celery.task(name="app.tasks.refresh.refresh_market_data_task")
def refresh_market_data_task():
    logger.info("Starting scheduled market data refresh.")

    db = SessionLocal()

    try:
        refresh_all_tickers_data(db)
        CacheManager(prefix="prices").clear()
        logger.info("Market data refresh complete.")
    except Exception as e:
        logger.error(f"Market data refresh failed: {e}", exc_info=True)
    finally:
        db.close()