import uvicorn
from dynaconf import Dynaconf

from mini_pipeline.api.app import create_app

if __name__ == '__main__':
    config = Dynaconf(settings_files=['app_config.yaml'])
    app = create_app(config)
    uvicorn.run(app, host="0.0.0.0", port=config.get("port", 8080))

