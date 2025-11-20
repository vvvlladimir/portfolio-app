from typing import List
from decouple import config


class Settings:
    # --- Database ---
    DB_USER: str = config("DB_USER", default="postgres")
    DB_PASSWORD: str = config("DB_PASSWORD", default="postgres")
    DB_NAME: str = config("DB_NAME", default="postgres")
    DB_HOST: str = config("DB_HOST", default="localhost")
    DB_PORT: int = config("DB_PORT", default=5432, cast=int)

    @property
    def DATABASE_URL(self) -> str:
        return (
            f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

    # --- Redis ---
    REDIS_HOST: str = config("REDIS_HOST", default="localhost")
    REDIS_PORT: int = config("REDIS_PORT", default=6379, cast=int)
    REDIS_DB: int = config("REDIS_DB", default=0, cast=int)
    REDIS_PASSWORD: str = config("REDIS_PASSWORD", default="")
    REDIS_USE_TLS: bool = config("REDIS_USE_TLS", default=False, cast=bool)

    @property
    def REDIS_URL(self) -> str:
        scheme = "rediss" if self.REDIS_USE_TLS else "redis"
        if self.REDIS_PASSWORD:
            return (
                f"{scheme}://:{self.REDIS_PASSWORD}@"
                f"{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
            )
        return f"{scheme}://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

    # --- Celery ---
    CELERY_BROKER_URL: str = config(
        "CELERY_BROKER_URL",
        default=f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
    )

    CELERY_RESULT_BACKEND: str = config(
        "CELERY_RESULT_BACKEND",
        default=f"redis://{REDIS_HOST}:{REDIS_PORT}/1"
    )

    CELERY_TIMEZONE: str = config("CELERY_TIMEZONE", default="UTC")
    CELERY_ENABLE_UTC: bool = config("CELERY_ENABLE_UTC", default=True, cast=bool)

    CELERY_BEAT_ENABLED: bool = config("CELERY_BEAT_ENABLED", default=True, cast=bool)

    CELERY_WORKER_CONCURRENCY: int = config(
        "CELERY_WORKER_CONCURRENCY", default=1, cast=int
    )

    # --- Logging & Debug ---
    SQL_ECHO: bool = config("SQL_ECHO", default=False, cast=bool)
    LOG_LEVEL: str = config("LOG_LEVEL", default="INFO").upper()
    SQL_LOG_LEVEL: str = config("SQL_LOG_LEVEL", default="WARNING").upper()
    UVICORN_LOG_LEVEL: str = config("UVICORN_LOG_LEVEL", default="info").upper()
    DEBUG: bool = config("DEBUG", default=False, cast=bool)

    # --- CORS ---
    CORS_ORIGINS: List[str] = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://192.168.0.51:3000",
    ]


settings = Settings()