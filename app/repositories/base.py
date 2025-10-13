import re
from typing import Generic, TypeVar, Type, List, Optional, Dict, Any, Union, Sequence, Callable
from slugify import slugify

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from app.core.db import Base
from app.core.logger import logger
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

T = TypeVar("T", bound=Base)

class RepositoryError(Exception):
    """Custom exception for repository operations"""
    pass


class BaseRepository(Generic[T]):
    def __init__(self, db: Session, model: Type[T]):
        self.db = db
        self.model = model

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

    # TODO: check is it effective than
    #         price_repo = factory.get_price_repository()
    #         stmt = select(Price)
    #         df_prices = pd.read_sql(stmt, price_repo.db.bind)
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

    @staticmethod
    def _snakeify(s: str) -> str:
        if not isinstance(s, str):
            return s
        s = s.strip()
        return slugify(s, separator="_", lowercase=True)

    @staticmethod
    def normalize_header(
            value: Optional[Union[str, List[str], pd.DataFrame, List[Dict[str, Any]]]]
    ) -> Optional[Union[str, List[str], pd.DataFrame, List[Dict[str, Any]]]]:
        if value is None:
            return None

        if isinstance(value, str):
            return BaseRepository._snakeify(value)

        if isinstance(value, list):
            if value and isinstance(value[0], dict):
                return [{BaseRepository._snakeify(k): v for k, v in rec.items()} for rec in value]
            return [BaseRepository._snakeify(v) for v in value]

        if isinstance(value, pd.DataFrame):
            df = value.copy()
            df.columns = [BaseRepository._snakeify(c) for c in df.columns]
            return df

        raise TypeError(
            f"Unsupported type for normalize_header: {type(value).__name__}. "
            "Expected str, list, pandas.DataFrame, or list of dictionaries."
        )

    def _validate_data(self, rows: List[Dict]) -> List[Dict]:
        """Basic validation function that normalizes headers and checks if data matches repository model."""
        if not rows:
            return []
        try:
            rows = self.normalize_header(rows)

            model_columns = set(column.name for column in self.model.__table__.columns)

            validated_rows = []
            for i, row in enumerate(rows):
                if not isinstance(row, dict):
                    logger.warning(f"Row {i} is not a dictionary, skipping")
                    continue

                valid_row = {}
                for key, value in row.items():
                    if key in model_columns:
                        valid_row[key] = value
                    else:
                        logger.debug(f"Row {i}: ignoring field '{key}' not in model {self.model.__name__}")

                if valid_row:
                    validated_rows.append(valid_row)
                else:
                    logger.warning(f"Row {i} has no valid fields for model {self.model.__name__}")

            return validated_rows

        except Exception as e:
            logger.error(f"Error validating data structure for {self.model.__name__}: {e}")
            raise RepositoryError(f"Failed to validate data structure") from e


    def upsert_bulk(
            self,
            data: Union[List[Dict], pd.DataFrame],
            index_elements: Optional[List[str]] = None,
            validate_fn: Optional[Callable[[List[Dict]], List[Dict]]] = None
    ) -> int:
        """Bulk upsert records with optional validation and conflict handling."""
        if data is None or len(data) == 0:
            return 0

        try:
            if isinstance(data, pd.DataFrame):
                data = data.to_dict(orient="records")

            data = self._validate_data(data)
            if validate_fn:
                data = validate_fn(data)

            if not data:
                return 0

            stmt = insert(self.model).values(data)
            if index_elements:
                stmt = stmt.on_conflict_do_nothing(index_elements=index_elements)

            result = self.db.execute(stmt)
            self.db.commit()

            return int(result.rowcount or 0)

        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"Error upserting {self.model.__name__}: {e}")
            raise RepositoryError(f"Failed to upsert {self.model.__name__}") from e