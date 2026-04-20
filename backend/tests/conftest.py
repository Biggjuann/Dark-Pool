"""
Shared pytest fixtures and path setup for the darkpool-tracker backend tests.
"""

import os
import sys

# Ensure `backend/` is on sys.path so tests can import modules the same way
# the app does (e.g. `from models import ...`, `from ingest.finra import ...`).
BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

# Point the app's database.py at an in-memory SQLite so tests never touch
# the real darkpool.db file.  Must be set before any app module is imported.
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database import Base


@pytest.fixture()
def db_session():
    """
    Yield a SQLAlchemy session bound to a fresh in-memory SQLite database.

    Each test gets a completely isolated DB — tables are created on entry and
    dropped on exit, so tests cannot interfere with each other.
    """
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
    )
    # Import models so their classes register on Base.metadata
    import models  # noqa: F401

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(engine)
        engine.dispose()
