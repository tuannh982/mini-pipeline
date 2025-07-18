mini-pipeline
===

## Prerequisites

- Python 3.12
- Poetry
- Docker

## Quickstarts

Starting infrastructures

```shell
docker compose -f docker/docker-compose.yaml
```

Running API server

```shell
poetry install
poetry run python src/mini_pipeline/main.py
```

### Running tests

```shell
poetry run pytest
```

## To be improved

- change local storage to s3
- run async transformation job