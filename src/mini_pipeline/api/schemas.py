import json
from typing import List

from pydantic import BaseModel

from mini_pipeline.core.template_types import Dataset, PipelineTemplate
from mini_pipeline.core.types import TransformationType


class PipelineTemplateSchema(BaseModel):
    name: str
    description: str = ""
    sources: List[Dataset]
    sinks: List[Dataset]
    transformations: List[TransformationType]

    @staticmethod
    def from_json(json_str: str) -> "PipelineTemplateSchema":
        parsed = json.loads(json_str)
        pipeline = PipelineTemplateSchema(**parsed)
        return pipeline

    def to_json(self) -> str:
        return self.model_dump_json()

    def to_pipeline_template(self) -> PipelineTemplate:
        return PipelineTemplate(**self.model_dump())


class PipelineTemplateResponse(PipelineTemplateSchema):
    id: int

    class Config:
        from_attributes = True


class FileSchema(BaseModel):
    path: str
    dataset: str

class ExecutePipelineRequest(BaseModel):
    template_id: int
    source_files: List[FileSchema]
    sink_files: List[FileSchema]
