import redis
from app.core.config import settings
from app.core.logger import logger

redis_client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)

def test_redis_connection():
    try:
        redis_client.ping()
        logger.info("Connected to Redis successfully.")
    except Exception as e:
        logger.error(f"Redis connection failed: {e}")