import json
from typing import List

from pydantic import BaseModel

from mini_pipeline.core.template_types import Dataset
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
        return self.model_dump()


class PipelineTemplateResponse(PipelineTemplateSchema):
    id: int

    class Config:
        from_attributes = True


class ExecutePipelineRequest(BaseModel):
    template_id: int
    source_files: List[str]
    sink_files: List[str]