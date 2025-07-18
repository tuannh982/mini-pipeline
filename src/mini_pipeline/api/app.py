import io
import json
from typing import List, Optional

from dynaconf import LazySettings
from fastapi import FastAPI, Depends, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse, JSONResponse
from sqlalchemy.orm import Session

from mini_pipeline.api.schemas import PipelineTemplateResponse, PipelineTemplateSchema, ExecutePipelineRequest
from mini_pipeline.common.conversion import csv_binary_to_json_binary
from mini_pipeline.common.file import extract_file_info
from mini_pipeline.common.logging import logger
from mini_pipeline.common.s3 import s3_client, create_bucket_if_not_exists
from mini_pipeline.core.types import Source, Sink
from mini_pipeline.db.tables import PipelineTemplateDB
from mini_pipeline.db.utils import Sessions
from mini_pipeline.pipeline_executor.spark_executor import execute_pipeline


def main_db():
    db = Sessions["main"]()
    try:
        yield db
    finally:
        db.close()


def create_app(config: LazySettings) -> FastAPI:
    app = FastAPI()
    # config
    s3_config = config.get("s3")
    if s3_config is None:
        raise AttributeError("'s3' is not defined")
    spark_config = config.get("spark")
    if spark_config is None:
        raise AttributeError("'spark' is not defined")

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
        bucket: str = s3_config.get("bucket")
        s3 = s3_client(config.s3)
        create_bucket_if_not_exists(s3, bucket)
        s3.put_object(Bucket=bucket, Key=file.filename, Body=contents)
        return {"filename": file.filename}

    @app.get("/datasets/download/{filename}")
    async def download_file(filename: str, mode: Optional[str] = None):
        bucket: str = s3_config.get("bucket")
        s3 = s3_client(config.s3)
        create_bucket_if_not_exists(s3, bucket)
        obj = s3.get_object(Bucket=bucket, Key=filename)
        obj_body = obj["Body"]
        match mode:
            case "json":
                json_bytes: bytes = csv_binary_to_json_binary(obj_body.read())
                json_obj = json.loads(json_bytes.decode('utf-8'))
                return JSONResponse(json_obj)
            case _:
                return StreamingResponse(obj_body, media_type='application/octet-stream')

    # Execution APIs
    @app.post("/execute-pipeline")
    async def execute(request: ExecutePipelineRequest, db: Session = Depends(main_db)):
        db_template: PipelineTemplateDB | None = db.query(PipelineTemplateDB).filter(
            PipelineTemplateDB.id == request.template_id).first()
        if not db_template:
            logger.warning(f"Template ID {request.template_id} not found.")
            raise HTTPException(status_code=404, detail="Template not found")
        template = PipelineTemplateSchema.from_json(db_template.json_definition).to_pipeline_template()
        sources: List[Source] = []
        sinks: List[Sink] = []
        for source_file in request.source_files:
            path = source_file.path
            file_info = extract_file_info(path)
            sources.append(Source(
                name=source_file.dataset,
                format=file_info.format,
                path=path,
                options={},
            ))
        for sink_file in request.sink_files:
            path = sink_file.path
            file_info = extract_file_info(path)
            sinks.append(Sink(
                input=sink_file.dataset,
                format=file_info.format,
                path=path,
                options={},
            ))
        pipeline = template.to_pipeline(sources=sources, sinks=sinks)
        print(pipeline.to_json())
        raise HTTPException(status_code=400, detail="ggwp")
        # spark_master = spark_config.get("masterUrl")
        # execute_pipeline(spark_master=spark_master, job_id=None, pipeline=db_template)
        # TODO

    return app
