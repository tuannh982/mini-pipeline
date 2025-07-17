from dynaconf import LazySettings
from fastapi import FastAPI


def create_app(config: LazySettings) -> FastAPI:
    app = FastAPI()

    @app.get("/ping")
    def ping():
        return {"message": "pong"}

    return app
