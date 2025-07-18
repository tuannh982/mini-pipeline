import io
import json
import os
import uuid
from typing import List, Optional, Dict

import pandas as pd
from dynaconf import LazySettings
from fastapi import FastAPI, Depends, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse, JSONResponse
from pandas import DataFrame
from pyspark.sql import SparkSession
from sqlalchemy.orm import Session

from mini_pipeline.api.schemas import PipelineTemplateResponse, PipelineTemplateSchema, ExecutePipelineRequest
from mini_pipeline.common.conversion import csv_binary_to_json_binary
from mini_pipeline.common.file import extract_file_info
from mini_pipeline.common.logging import logger
from mini_pipeline.core.types import Source, Sink
from mini_pipeline.db.tables import PipelineTemplateDB
from mini_pipeline.db.utils import Sessions
from mini_pipeline.pipeline_executor.spark_executor import execute_pipeline, execute_pipeline_with_session


def main_db():
    db = Sessions["main"]()
    try:
        yield db
    finally:
        db.close()


def create_app(config: LazySettings) -> FastAPI:
    app = FastAPI()
    # config
    storage_config = config.get("storage")
    local_storage_config = storage_config.get("local")
    spark_config = config.get("spark")

    # health APIs
    @app.get("/health", response_model=str)
    async def health():
        return "ok"

    # template APIs
    @app.post("/pipeline-templates", response_model=PipelineTemplateResponse)
    async def create_pipeline_template(template: PipelineTemplateSchema, db: Session = Depends(main_db)):
        db_template = PipelineTemplateDB(
            name=template.name,
            description=template.description,
            json_definition=template.to_json()
        )
        db.add(db_template)
        db.commit()
        db.refresh(db_template)
        return PipelineTemplateResponse(
            id=db_template.id,
            **template.model_dump(),
        )

    @app.get("/pipeline-templates/{template_id}", response_model=PipelineTemplateResponse)
    async def get_pipeline_template(template_id: int, db: Session = Depends(main_db)):
        db_template: PipelineTemplateDB | None = db.query(PipelineTemplateDB).filter(
            PipelineTemplateDB.id == template_id).first()
        if not db_template:
            logger.warning(f"Template ID {template_id} not found.")
            raise HTTPException(status_code=404, detail="Template not found")
        template = PipelineTemplateSchema.from_json(db_template.json_definition)
        return PipelineTemplateResponse(
            id=db_template.id,
            **template.model_dump(),
        )

    # Dataset APIs
    @app.post("/datasets/upload")
    async def upload_file(file: UploadFile = File(...)):
        contents = await file.read()
        local_storage_path = local_storage_config.get("path")
        file_location = f"{local_storage_path}/" + file.filename
        with open(file_location, "wb") as f:
            f.write(contents)
        return {"filename": file.filename}

    @app.get("/datasets/download/{filename}")
    async def download_file(filename: str, mode: Optional[str] = None):
        local_storage_path = local_storage_config.get("path")
        file_location = os.path.join(local_storage_path, filename)
        if not os.path.exists(file_location):
            raise HTTPException(status_code=404, detail="File not found")
        if mode == "json":
            with open(file_location, "rb") as f:
                csv_bytes = f.read()
            json_bytes = csv_binary_to_json_binary(csv_bytes)
            json_obj = json.loads(json_bytes.decode('utf-8'))
            return JSONResponse(json_obj)
        else:
            file_stream = open(file_location, mode="rb")
            return StreamingResponse(file_stream, media_type="application/octet-stream")

    # Execution APIs
    @app.post("/execute-pipeline")
    async def execute(request: ExecutePipelineRequest, db: Session = Depends(main_db)):
        db_template: PipelineTemplateDB | None = db.query(PipelineTemplateDB).filter(
            PipelineTemplateDB.id == request.template_id).first()
        if not db_template:
            logger.warning(f"Template ID {request.template_id} not found.")
            raise HTTPException(status_code=404, detail="Template not found")
        local_storage_path = local_storage_config.get("path")
        template = PipelineTemplateSchema.from_json(db_template.json_definition).to_pipeline_template()
        spark_master = spark_config.get("masterUrl")
        sources: List[Source] = []
        sinks: List[Sink] = []
        for source_file in request.source_files:
            path = source_file.path
            file_info = extract_file_info(path)
            sources.append(Source(
                name=source_file.dataset,
                format=file_info.format,
                path=f"{local_storage_path}/{path}",
                options={
                    "header": "true",
                    "inferSchema": "true",
                },
            ))
        for sink_file in request.sink_files:
            path = sink_file.path
            file_info = extract_file_info(path)
            sinks.append(Sink(
                input=sink_file.dataset,
                format=file_info.format,
                path=f"{local_storage_path}/{path}",
                options={
                    "header": "true",
                    "inferSchema": "true",
                },
            ))
        pipeline = template.to_pipeline(sources=sources, sinks=sinks)
        execute_pipeline(spark_master, None, pipeline)
        return "ok"

    return app
