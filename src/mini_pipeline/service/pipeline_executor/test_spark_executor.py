import pytest
from pyspark.sql import SparkSession, DataFrame

from mini_pipeline.service.pipeline_executor.spark_executor import apply_transformations


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("TestJob") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_apply_transformation(spark):
    # init sample data
    data = [("A", 1), ("B", 2), ("C", 3)]
    df = spark.createDataFrame(data, ["name", "age"])
    pipeline = Pipeline(
        sources=[],
        sinks=[],
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
