import base64
import json

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import expr

from mini_pipeline.core.types import *


def parse_pipeline(base64_encoded_json_pipeline: str) -> Pipeline:
    json_str = base64.b64decode(base64_encoded_json_pipeline)
    parsed = json.loads(json_str)
    pipeline = Pipeline(**parsed)
    return pipeline


def apply_transformation(t: TransformationType, dataframes: dict[str, DataFrame]) -> DataFrame:
    match t:
        case Select(input=inp, columns=cols):
            df = dataframes[inp]
            return df.select(*cols)
        case Filter(input=inp, condition=cond):
            df = dataframes[inp]
            return df.filter(cond)
        case Map(input=inp, new_column=col, expression=expression):
            df = dataframes[inp]
            return df.withColumn(col, expr(expression))
        case Reduce(input=inp, group_by=group, aggregation=agg):
            df = dataframes[inp]
            agg_f = [expr(v).alias(k) for k, v in agg.items()]
            return df.groupBy(*group).agg(*agg_f)
        case Join(left=l, right=r, on=on_key, how=how):
            df_l = dataframes[l]
            df_r = dataframes[r]
            return df_l.join(df_r, on=on_key, how=how)
        case Merge(left=l, right=r):
            df_l = dataframes[l]
            df_r = dataframes[r]
            return df_l.unionByName(df_r)
        case _:
            raise Exception(f"Unsupported transformation: {t}")


def apply_transformations(pipeline: Pipeline, dataframes: dict[str, DataFrame]) -> None:
    for t in pipeline.transformations:
        df = apply_transformation(t, dataframes)
        dataframes[t.output] = df


def execute_pipeline_with_session(spark: SparkSession, base64_encoded_json_pipeline: str) -> None:
    pipeline = parse_pipeline(base64_encoded_json_pipeline)
    dataframes: dict[str, DataFrame] = {}
    # source
    for source in pipeline.sources:
        df = spark.read.format(source.format).options(**source.options).load(source.path)
        dataframes[source.name] = df
    # transform
    apply_transformations(pipeline, dataframes)
    # sink
    sink = pipeline.sink
    sink_df = dataframes[pipeline.sink.input]
    sink_df.write.mode("overwrite").format(sink.format).options(**sink.options).save(sink.path)


def execute_pipeline(spark_master: str, job_id: str, base64_encoded_json_pipeline: str) -> None:
    spark: SparkSession | None = None
    try:
        spark = (SparkSession.builder
                 .appName(job_id)
                 .master(spark_master)
                 .getOrCreate())
        execute_pipeline_with_session(spark, base64_encoded_json_pipeline)
    finally:
        spark.stop()
