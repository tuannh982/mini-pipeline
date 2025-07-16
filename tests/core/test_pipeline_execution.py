import pytest
from pyspark.sql import SparkSession, DataFrame

from mini_pipeline.core.pipeline_execution import apply_transformations
from mini_pipeline.core.types import *


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("TestJob") \
        .master("spark://localhost:7077") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_apply_transformation(spark):
    # init sample data
    data = [("A", 1), ("B", 2), ("C", 3)]
    df = spark.createDataFrame(data, ["name", "age"])
    pipeline = Pipeline(
        sources=[],
        sink=Sink(input="", format="", path="", options={}),
        transformations=[
            Filter(
                operation="Filter",
                input="source",
                output="df_node_1",
                condition="age > 1"
            ),
            Map(
                operation="Map",
                input="df_node_1",
                output="df_node_2",
                new_column="is_two",
                expression="age == 2"
            )
        ]
    )
    #
    dataframes: dict[str, DataFrame] = {"source": df}
    apply_transformations(pipeline, dataframes)
    result = dataframes["df_node_2"].collect()
    result_names = sorted([row["name"] for row in result])
    expected_names = ["B", "C"]
    assert result_names == expected_names
