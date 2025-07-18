import json
from typing import List, Dict, Union, Literal
from pydantic import BaseModel


class Source(BaseModel):
    name: str
    format: str
    path: str
    options: Dict[str, str]


class Sink(BaseModel):
    input: str
    format: str
    path: str
    options: Dict[str, str]


class Transformation(BaseModel):
    operation: str
    output: str
    model_config = {
        "extra": "forbid",
        "discriminator": "operation"
    }


# transformation definitions
class Select(Transformation):
    operation: Literal["Select"] = "Select"
    input: str
    columns: List[str]
    enabled: bool = True


class Filter(Transformation):
    operation: Literal["Filter"] = "Filter"
    input: str
    condition: str
    enabled: bool = True


class Map(Transformation):
    operation: Literal["Map"] = "Map"
    input: str
    new_column: str
    expression: str
    enabled: bool = True


class Reduce(Transformation):
    operation: Literal["Reduce"] = "Reduce"
    input: str
    group_by: List[str]
    aggregation: Dict[str, str]
    enabled: bool = True


class Join(Transformation):
    operation: Literal["Join"] = "Join"
    left: str
    right: str
    on: str
    how: str


class Merge(Transformation):
    operation: Literal["Merge"] = "Merge"
    left: str
    right: str


TransformationType = Union[Select, Filter, Map, Reduce, Join, Merge]


# pipeline
class Pipeline(BaseModel):
    sources: List[Source]
    sinks: List[Sink]
    transformations: List[TransformationType]

    @staticmethod
    def from_json(json_str: str) -> "Pipeline":
        parsed = json.loads(json_str)
        pipeline = Pipeline(**parsed)
        return pipeline

    def to_json(self) -> str:
        return self.model_dump_json()