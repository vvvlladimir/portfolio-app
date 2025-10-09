from typing import List
from decouple import config


class Settings:
    DB_USER: str = config("DB_USER", default="postgres")
    DB_PASSWORD: str = config("DB_PASSWORD", default="postgres")
    DB_NAME: str = config("DB_NAME", default="postgres")
    DB_HOST: str = config("DB_HOST", default="localhost")
    DB_PORT: int = config("DB_PORT", default=5432, cast=int)

    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    SQL_ECHO: bool = config("SQL_ECHO", default=False, cast=bool)

    LOG_LEVEL: str = config("LOG_LEVEL", default="INFO").upper()
    SQL_LOG_LEVEL: str = config("SQL_LOG_LEVEL", default="WARNING").upper()
    UVICORN_LOG_LEVEL: str = config("UVICORN_LOG_LEVEL", default="info").upper()

    DEBUG: bool = config("DEBUG", default=False, cast=bool)

    CORS_ORIGINS: List[str] = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://192.168.0.51:3000",
    ]


settings = Settings()