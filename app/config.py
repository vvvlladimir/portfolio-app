from typing import List


class Settings:
    CORS_ORIGINS: List[str] = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://192.168.0.51:3000"
    ]

settings = Settings()