from typing import Type, TypeVar, Dict
from sqlalchemy.orm import Session
from app.repositories.base import BaseRepository
from app.repositories.portfolio_history import PortfolioHistoryRepository
from app.repositories.positions import PositionRepository
from app.repositories.prices import PriceRepository
from app.repositories.transactions import TransactionRepository
from app.repositories.ticker import TickerRepository

T = TypeVar('T', bound=BaseRepository)


class RepositoryFactory:
    """
    Factory class for creating repository instances with dependency injection.
    Provides a centralized way to manage repository creation and configuration.
    """

    _repository_mapping: Dict[str, Type[BaseRepository]] = {
        'portfolio_history': PortfolioHistoryRepository,
        'positions': PositionRepository,
        'prices': PriceRepository,
        'transactions': TransactionRepository,
        'ticker': TickerRepository,
    }

    def __init__(self, db: Session):
        self.db = db
        self._instances: Dict[str, BaseRepository] = {}

    def get_repository(self, repository_name: str) -> BaseRepository:
        """
        Get a repository instance by name. Creates a singleton instance per factory.

        Args:
            repository_name: Name of the repository ('portfolio_history', 'positions', etc.)

        Returns:
            Repository instance

        Raises:
            ValueError: If repository name is not recognized
        """
        if repository_name not in self._repository_mapping:
            available = ', '.join(self._repository_mapping.keys())
            raise ValueError(f"Unknown repository '{repository_name}'. Available: {available}")

        if repository_name not in self._instances:
            repository_class = self._repository_mapping[repository_name]
            self._instances[repository_name] = repository_class(self.db)

        return self._instances[repository_name]

    def get_portfolio_history_repository(self) -> PortfolioHistoryRepository:
        """Get PortfolioHistoryRepository instance"""
        return self.get_repository('portfolio_history')

    def get_position_repository(self) -> PositionRepository:
        """Get PositionRepository instance"""
        return self.get_repository('positions')

    def get_price_repository(self) -> PriceRepository:
        """Get PriceRepository instance"""
        return self.get_repository('prices')

    def get_transaction_repository(self) -> TransactionRepository:
        """Get TransactionRepository instance"""
        return self.get_repository('transactions')

    def get_ticker_repository(self) -> TickerRepository:
        """Get TickerRepository instance"""
        return self.get_repository('ticker')

    @classmethod
    def register_repository(cls, name: str, repository_class: Type[BaseRepository]) -> None:
        """
        Register a new repository class with the factory.

        Args:
            name: Name to register the repository under
            repository_class: The repository class to register
        """
        cls._repository_mapping[name] = repository_class

    def clear_cache(self) -> None:
        """Clear all cached repository instances"""
        self._instances.clear()


def get_repository_factory(db: Session) -> RepositoryFactory:
    """
    Convenience function to create a RepositoryFactory instance.

    Args:
        db: SQLAlchemy database session

    Returns:
        Configured RepositoryFactory instance
    """
    return RepositoryFactory(db)
