"""create hypertable prices and transactions

Revision ID: 71bd60b0c072
Revises:
Create Date: 2025-10-04 16:33:59.753406

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '71bd60b0c072'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")

    op.execute("""
        SELECT create_hypertable('transactions', 'date', if_not_exists => TRUE, migrate_data => TRUE);
    """)
    op.execute("""
        SELECT create_hypertable('prices', 'date', if_not_exists => TRUE, migrate_data => TRUE);
    """)
    op.execute("""
        SELECT create_hypertable('portfolio_history', 'date', if_not_exists => TRUE, migrate_data => TRUE);
    """)


def downgrade():
    pass