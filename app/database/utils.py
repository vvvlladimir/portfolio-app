from sqlalchemy.orm import Session
from sqlalchemy import text


def refresh_materialized_view(db: Session, view_name: str, concurrently: bool = True):
    """
    Refresh a materialized view in the database.
    """
    if not view_name.isidentifier():
        raise ValueError(f"Invalid view name: {view_name}")

    sql = f"REFRESH MATERIALIZED VIEW {'CONCURRENTLY ' if concurrently else ''}{view_name};"

    try:
        db.execute(text(sql))
        db.commit()
    except Exception as e:
        db.rollback()
        raise RuntimeError(f"Failed to refresh materialized view '{view_name}': {str(e)}")