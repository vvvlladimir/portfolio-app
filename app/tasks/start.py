from celery.signals import worker_ready
from app.core.logger import logger
from app.tasks.refresh import refresh_market_data_task


@worker_ready.connect
def at_worker_start(sender, **kwargs):
    """
    Automatically trigger when the Celery worker starts.
    """
    logger.info("Celery Worker is ready")

    logger.info("Celery - triggering price refresh.")
    refresh_market_data_task.delay()