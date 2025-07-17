from dynaconf import LazySettings
from fastapi import FastAPI, Depends, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from mini_pipeline.api.schemas import PipelineTemplateResponse, PipelineTemplateSchema, ExecutePipelineRequest
from mini_pipeline.common.logging import logger
from mini_pipeline.common.s3 import s3_client, create_bucket_if_not_exists
from mini_pipeline.db.tables import PipelineTemplateDB
from mini_pipeline.db.utils import Sessions


def main_db():
    db = Sessions["main"]()
    try:
        yield db
    finally:
        db.close()


def create_app(config: LazySettings) -> FastAPI:
    app = FastAPI()

    # health APIs
    @app.get("/health", response_model=str)
    def health():
        return "ok"

    # template APIs
    @app.post("/pipeline-templates", response_model=PipelineTemplateResponse)
    def create_pipeline_template(template: PipelineTemplateSchema, db: Session = Depends(main_db)):
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
    def get_pipeline_template(template_id: int, db: Session = Depends(main_db)):
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
        s3_config = config.get("s3")
        bucket: str = s3_config.get("bucket")
        s3 = s3_client(config.s3)
        create_bucket_if_not_exists(s3, bucket)
        s3.put_object(Bucket=bucket, Key=file.filename, Body=contents)
        return {"filename": file.filename}

    @app.get("/datasets/download/{filename}")
    def download_file(filename: str):
        s3_config = config.get("s3")
        bucket: str = s3_config.get("bucket")
        s3 = s3_client(config.s3)
        create_bucket_if_not_exists(s3, bucket)
        obj = s3.get_object(Bucket=bucket, Key=filename)
        return StreamingResponse(obj['Body'], media_type='application/octet-stream')

    # Execution APIs
    @app.post("/execute-pipeline")
    def execute_pipeline(request: ExecutePipelineRequest, db: Session = Depends(main_db)):
        db_template: PipelineTemplateDB | None = db.query(PipelineTemplateDB).filter(
            PipelineTemplateDB.id == request.template_id).first()
        # TODO

    return app
