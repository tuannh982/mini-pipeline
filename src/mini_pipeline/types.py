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
    operation: Literal["Select"]
    input: str
    columns: List[str]


class Filter(Transformation):
    operation: Literal["Filter"]
    input: str
    condition: str


class Map(Transformation):
    operation: Literal["Map"]
    input: str
    new_column: str
    expression: str


class Reduce(Transformation):
    operation: Literal["Reduce"]
    input: str
    group_by: List[str]
    aggregation: Dict[str, str]


class Join(Transformation):
    operation: Literal["Join"]
    left: str
    right: str
    on: str
    how: str


class Merge(Transformation):
    operation: Literal["Merge"]
    left: str
    right: str


TransformationType = Union[Select, Filter, Map, Reduce, Join, Merge]


# pipeline
class Pipeline(BaseModel):
    sources: List[Source]
    sink: Sink
    transformations: List[TransformationType]
