"""FastAPI app: database-as-JSON API with Bearer auth."""

import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from sqlalchemy import create_engine

# Ensure project root is on path for scripts.keyvault_loader
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from api.routes import router
from api import db


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load env (Key Vault), create engine, validate required env, then yield."""
    from scripts.keyvault_loader import load_env

    load_env()
    if not os.environ.get("DATABASE_URL"):
        raise RuntimeError("DATABASE_URL is not set (Key Vault or .env)")
    if not os.environ.get("API_AUTH_TOKEN"):
        raise RuntimeError("API_AUTH_TOKEN is not set (Key Vault or .env)")
    database_url = os.environ["DATABASE_URL"]
    connect_args = {"timeout": 10} if database_url.startswith("mssql+") else {"connect_timeout": 10}
    engine = create_engine(
        database_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        connect_args=connect_args,
    )
    db.set_engine(engine)
    yield
    engine.dispose()


app = FastAPI(title="Database JSON API", lifespan=lifespan)
app.include_router(router)
