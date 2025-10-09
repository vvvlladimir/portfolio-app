from fastapi import Depends
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.repositories.factory import RepositoryFactory


def get_factory(db: Session = Depends(get_db)) -> RepositoryFactory:
    return RepositoryFactory(db)