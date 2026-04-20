"""
Database engine, session factory, and initialisation helpers.

Environment variables
---------------------
DATABASE_URL  — SQLAlchemy connection string (default: sqlite:///./darkpool.db)
"""

import logging
import os

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import declarative_base, sessionmaker

logger = logging.getLogger(__name__)

DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./darkpool.db")

# Railway provides postgres:// URLs; SQLAlchemy 2.x requires postgresql://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

_connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

engine = create_engine(
    DATABASE_URL,
    connect_args=_connect_args,
    # Echo SQL to stdout when running under DEBUG logging
    echo=os.getenv("SQL_ECHO", "0") == "1",
)

# Enable WAL mode for SQLite so reads don't block the weekly write job
if DATABASE_URL.startswith("sqlite"):
    @event.listens_for(engine, "connect")
    def _set_wal_mode(dbapi_conn, _):
        dbapi_conn.execute("PRAGMA journal_mode=WAL")
        dbapi_conn.execute("PRAGMA foreign_keys=ON")
        dbapi_conn.execute("PRAGMA busy_timeout=15000")  # wait up to 15 s before giving up


# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# All models import Base from here; it must be defined before models.py loads
Base = declarative_base()


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------

def get_db():
    """
    Yield a SQLAlchemy session, then close it after the request.

    Usage::

        @router.get("/")
        def endpoint(db: Session = Depends(get_db)):
            ...
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------------------
# First-run initialisation
# ---------------------------------------------------------------------------

def db_init() -> None:
    """
    Create all tables that don't yet exist.

    Safe to call on every startup — SQLAlchemy uses CREATE TABLE IF NOT EXISTS
    under the hood, so existing data is never touched.

    For schema migrations after initial deployment use Alembic instead:
        alembic upgrade head
    """
    # Import models here so Base.metadata is populated before create_all runs.
    import models  # noqa: F401

    Base.metadata.create_all(bind=engine)
    logger.info("db_init: tables verified / created against %s", DATABASE_URL)
