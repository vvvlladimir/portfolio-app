from typing import Generic, TypeVar, Type, List, Optional, Dict, Any, Union
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from app.core.db import Base
from app.core.logger import logger

T = TypeVar("T", bound=Base)



class RepositoryError(Exception):
    """Custom exception for repository operations"""
    pass


class BaseRepository(Generic[T]):
    def __init__(self, db: Session, model: Type[T]):
        self.db = db
        self.model = model

    @staticmethod
    def normalize(value: Optional[str]) -> Optional[str]:
        """Normalize string values for consistent filtering"""
        return value.upper().strip() if isinstance(value, str) else None

    def get(self, id_: Union[int, str]) -> Optional[T]:
        """Get a single record by ID"""
        try:
            return self.db.get(self.model, id_)
        except SQLAlchemyError as e:
            logger.error(f"Error getting {self.model.__name__} with id {id_}: {e}")
            raise RepositoryError(f"Failed to get {self.model.__name__}") from e

    def get_by_filters(self, **filters) -> List[T]:
        """Get records by multiple filter criteria"""
        try:
            query = self.db.query(self.model)
            for key, value in filters.items():
                if hasattr(self.model, key):
                    query = query.filter(getattr(self.model, key) == value)
            return query.all()
        except SQLAlchemyError as e:
            logger.error(f"Error filtering {self.model.__name__}: {e}")
            raise RepositoryError(f"Failed to filter {self.model.__name__}") from e

    def get_all(self, limit: Optional[int] = None, offset: Optional[int] = None) -> List[T]:
        """Get all records with optional pagination"""
        try:
            query = self.db.query(self.model)
            if offset:
                query = query.offset(offset)
            if limit:
                query = query.limit(limit)
            return query.all()
        except SQLAlchemyError as e:
            logger.error(f"Error getting all {self.model.__name__}: {e}")
            raise RepositoryError(f"Failed to get all {self.model.__name__}") from e

    def create(self, obj_in: Dict[str, Any]) -> T:
        """Create a new record"""
        try:
            db_obj = self.model(**obj_in)
            self.db.add(db_obj)
            self.db.commit()
            self.db.refresh(db_obj)
            return db_obj
        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"Error creating {self.model.__name__}: {e}")
            raise RepositoryError(f"Failed to create {self.model.__name__}") from e

    def update(self, id_: Union[int, str], obj_in: Dict[str, Any]) -> Optional[T]:
        """Update an existing record"""
        try:
            db_obj = self.get(id_)
            if not db_obj:
                return None

            for field, value in obj_in.items():
                if hasattr(db_obj, field):
                    setattr(db_obj, field, value)

            self.db.commit()
            self.db.refresh(db_obj)
            return db_obj
        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"Error updating {self.model.__name__} with id {id_}: {e}")
            raise RepositoryError(f"Failed to update {self.model.__name__}") from e

    def delete(self, id_: Union[int, str]) -> bool:
        """Delete a record by ID"""
        try:
            db_obj = self.get(id_)
            if not db_obj:
                return False

            self.db.delete(db_obj)
            self.db.commit()
            return True
        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"Error deleting {self.model.__name__} with id {id_}: {e}")
            raise RepositoryError(f"Failed to delete {self.model.__name__}") from e

    def delete_all(self) -> int:
        """Delete all records"""
        try:
            deleted = self.db.query(self.model).delete(synchronize_session=False)
            self.db.commit()
            return int(deleted or 0)
        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"Error deleting all {self.model.__name__}: {e}")
            raise RepositoryError(f"Failed to delete all {self.model.__name__}") from e

    def exists(self, id_: Union[int, str]) -> bool:
        """Check if record exists"""
        try:
            return self.db.query(self.model).filter(
                getattr(self.model, self.model.__table__.primary_key.columns.keys()[0]) == id_
            ).first() is not None
        except SQLAlchemyError as e:
            logger.error(f"Error checking existence of {self.model.__name__} with id {id_}: {e}")
            raise RepositoryError(f"Failed to check existence of {self.model.__name__}") from e

    def count(self) -> int:
        """Count total records"""
        try:
            return self.db.query(self.model).count()
        except SQLAlchemyError as e:
            logger.error(f"Error counting {self.model.__name__}: {e}")
            raise RepositoryError(f"Failed to count {self.model.__name__}") from e
