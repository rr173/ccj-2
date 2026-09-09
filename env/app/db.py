from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.config import settings


def build_engine(pool_size: int | None = None, max_overflow: int | None = None) -> Engine:
    return create_engine(
        settings.database_url,
        pool_size=pool_size if pool_size is not None else settings.db_pool_size,
        max_overflow=(
            max_overflow if max_overflow is not None else settings.db_max_overflow
        ),
        pool_pre_ping=True,
        future=True,
    )


engine = build_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
