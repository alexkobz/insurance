import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text as sa_text

from functions.path import get_project_root, Path

env_path: Path = Path.joinpath(get_project_root(), '.venv/.env')
load_dotenv(env_path)
try:
    DATABASE_URI: str = (
        f"postgresql://"
        f"{os.environ['POSTGRES_USER']}:"
        f"{os.environ['POSTGRES_PASSWORD']}@"
        f"{os.environ['POSTGRES_HOST']}:"
        f"{os.environ['POSTGRES_PORT']}/"
        f"{os.environ['POSTGRES_DATABASE']}")
    engine = create_engine(DATABASE_URI)
    con = engine.connect()
    con.execute(sa_text(f'''SELECT 1''').execution_options(autocommit=True))
except SQLAlchemyError:
    DATABASE_URI: str = (
        f"postgresql://"
        f"{os.environ['POSTGRES_USER']}:"
        f"{os.environ['POSTGRES_PASSWORD']}@"
        f"localhost:"
        f"{os.environ['POSTGRES_PORT']}/"
        f"{os.environ['POSTGRES_DATABASE']}")
    engine = create_engine(DATABASE_URI)
