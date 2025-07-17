from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

Sessions: dict[str, sessionmaker[Session]] = {}


def init_db(key: str, db_config) -> None:
    driver = db_config.get("driver")
    user = db_config.get("user")
    password = db_config.get("password")
    host = db_config.get("host")
    port = db_config.get("port")
    database = db_config.get("database")

    if driver is None:
        raise AttributeError("'driver' is not defined")
    if user is None:
        raise AttributeError("'user' is not defined")
    if password is None:
        raise AttributeError("'password' is not defined")
    if host is None:
        raise AttributeError("'host' is not defined")
    if port is None:
        raise AttributeError("'port' is not defined")
    if database is None:
        raise AttributeError("'database' is not defined")

    database_url = f"{driver}ql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(database_url)
    maker = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Sessions[key] = maker


def init_db_from_config(config: dict) -> None:
    for key, value in config.items():
        if value is not None:
            init_db(key, value)
