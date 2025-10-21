from typing import Any, Optional
from app.core.redis_client import redis_client
from app.core.logger import logger
import json
from app.scripts.json_utils import json_serializer


class CacheManager:
    def __init__(self, prefix: str = ""):
        self.prefix = prefix.rstrip(":")

    def _build_key(self, *parts: Any, user_id: Optional[int] = None) -> str:
        """builds a cache key: snapshot:user:42:2025-10-21 or positions:global:list"""
        segments = [self.prefix]
        if user_id is not None:
            segments.append(f"user:{user_id}")
        segments.extend(str(p) for p in parts if p is not None)
        return ":".join(segments)

    def get(self, *parts, user_id: Optional[int] = None):
        key = self._build_key(*parts, user_id=user_id)
        data = redis_client.get(key)
        if data:
            logger.debug(f"Cache hit: {key}")
            return json.loads(data)
        logger.debug(f"Cache miss: {key}")
        return None

    def set(self, data: Any, *parts, user_id: Optional[int] = None, ttl: int = 300):
        key = self._build_key(*parts, user_id=user_id)
        redis_client.set(key, json.dumps(data, default=json_serializer), ex=ttl)
        logger.debug(f"Cache set: {key} (TTL={ttl}s)")

    def delete(self, *parts, user_id: Optional[int] = None):
        key = self._build_key(*parts, user_id=user_id)
        redis_client.delete(key)
        logger.debug(f"Cache deleted: {key}")

    def clear(self, pattern: Optional[str] = None):
        """Delete all cache entries matching the given pattern."""
        pattern = pattern or f"{self.prefix}*"
        count = 0
        for key in redis_client.scan_iter(pattern):
            redis_client.delete(key)
            count += 1
        logger.info(f"Cleared {count} cache entries for pattern '{pattern}'")
        return count