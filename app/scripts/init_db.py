from app.core.db import engine, Base
from app import models

def init_db():
    # Base.metadata.create_all(bind=engine)
    pass

if __name__ == "__main__":
    init_db()