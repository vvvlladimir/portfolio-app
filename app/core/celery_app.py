# app/core/celery_app.py
from celery import Celery
from celery.schedules import crontab
from app.core.config import settings


def make_celery() -> Celery:
    celery = Celery(
        "app",
        broker=settings.CELERY_BROKER_URL,
        backend=settings.CELERY_RESULT_BACKEND,
        include=[
            "app.tasks.refresh",
            "app.tasks.start",
        ],
    )

    celery.conf.update(
        timezone=settings.CELERY_TIMEZONE,
        enable_utc=settings.CELERY_ENABLE_UTC,
        broker_connection_retry_on_startup=True,
        worker_concurrency=settings.CELERY_WORKER_CONCURRENCY,
    )

    if settings.CELERY_BEAT_ENABLED:
        celery.conf.beat_schedule = {
            'refresh-market-data-daily': {
                'task': 'app.tasks.refresh.refresh_market_data_task',
                'schedule': crontab(minute='10', hour='0'),
            },
        }
        celery.conf.beat_max_loop_interval = 10

    return celery


celery = make_celery()
