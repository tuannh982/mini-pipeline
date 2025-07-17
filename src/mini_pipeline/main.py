import uvicorn

from mini_pipeline.api.app import create_app
from mini_pipeline.common.config import config
from mini_pipeline.db.utils import init_db_from_config

if __name__ == '__main__':
    # init db sessions
    if "db" in config:
        init_db_from_config(config.get("db"))
    # init api server
    app = create_app(config)
    uvicorn.run(app, host="0.0.0.0", port=config.get("port", 8080))
