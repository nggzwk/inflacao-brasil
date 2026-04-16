from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker


def get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")
    return database_url


def create_db_engine(database_url: str | None = None) -> Engine:
    return create_engine(
        database_url or get_database_url(),
        pool_pre_ping=True,
        future=True,
    )


_engine: Engine | None = None
SessionLocal = sessionmaker(autoflush=False, autocommit=False, expire_on_commit=False)


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_db_engine()
        SessionLocal.configure(bind=_engine)
    return _engine


def init_session_factory(database_url: str | None = None) -> Engine:
    global _engine
    _engine = create_db_engine(database_url)
    SessionLocal.configure(bind=_engine)
    return _engine


@contextmanager
def get_session() -> Generator[Session, None, None]:
    if SessionLocal.kw.get("bind") is None:
        get_engine()

    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def refresh_item_monthly_price(session: Session) -> None:
    session.execute(text("SELECT inflacao_brasil.refresh_item_monthly_price()"))
