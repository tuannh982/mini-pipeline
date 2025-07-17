import json

from mini_pipeline.core.types import *


def test_pipeline_codec():
    json_str = """
    {
      "sources": [
        {
          "name": "input_data",
          "format": "csv",
          "path": "/data/input.csv",
          "options": {}
        }
      ],
      "sinks": [ 
        {
            "input": "final_output",
            "format": "parquet",
            "path": "/data/output.parquet",
            "options": {}
          }
      ],
      "transformations": [
        {
          "operation": "Select",
          "output": "selected_data",
          "input": "input_data",
          "columns": ["id", "name", "age"],
          "enabled": false
        },
        {
          "operation": "Filter",
          "output": "filtered_data",
          "input": "selected_data",
          "condition": "age > 21",
          "enabled": true
        },
        {
          "operation": "Map",
          "output": "mapped_data",
          "input": "filtered_data",
          "new_column": "is_adult",
          "expression": "age >= 18",
          "enabled": true
        },
        {
          "operation": "Reduce",
          "output": "aggregated_data",
          "input": "mapped_data",
          "group_by": ["is_adult"],
          "aggregation": {
            "count": "count(*)",
            "avg_age": "avg(age)"
          },
          "enabled": true
        },
        {
          "operation": "Join",
          "output": "joined_data",
          "left": "aggregated_data",
          "right": "some_other_data",
          "on": "id",
          "how": "inner"
        },
        {
          "operation": "Merge",
          "output": "final_output",
          "left": "joined_data",
          "right": "extra_data"
        }
      ]
    }
    """
    expected = Pipeline(
        sources=[
            Source(
                name="input_data",
                format="csv",
                path="/data/input.csv",
                options={}
            )
        ],
        sinks=[
            Sink(
                input="final_output",
                format="parquet",
                path="/data/output.parquet",
                options={}
            ),
        ],
        transformations=[
            Select(
                output="selected_data",
                input="input_data",
                columns=["id", "name", "age"],
                enabled=False,
            ),
            Filter(
                output="filtered_data",
                input="selected_data",
                condition="age > 21"
            ),
            Map(
                output="mapped_data",
                input="filtered_data",
                new_column="is_adult",
                expression="age >= 18"
            ),
            Reduce(
                output="aggregated_data",
                input="mapped_data",
                group_by=["is_adult"],
                aggregation={
                    "count": "count(*)",
                    "avg_age": "avg(age)"
                }
            ),
            Join(
                output="joined_data",
                left="aggregated_data",
                right="some_other_data",
                on="id",
                how="inner"
            ),
            Merge(
                output="final_output",
                left="joined_data",
                right="extra_data"
            )
        ]
    )
    parsed = json.loads(json_str)
    actual = Pipeline(**parsed)
    assert actual == expected
