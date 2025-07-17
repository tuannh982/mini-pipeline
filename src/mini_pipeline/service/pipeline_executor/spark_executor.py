import uuid

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import expr

from mini_pipeline.domain.core.types import *


def apply_transformation(t: TransformationType, dataframes: dict[str, DataFrame]) -> DataFrame:
    match t:
        case Select(input=inp, columns=cols, enabled=enabled):
            df = dataframes[inp]
            if not enabled:
                return df
            return df.select(*cols)
        case Filter(input=inp, condition=cond, enabled=enabled):
            df = dataframes[inp]
            if not enabled:
                return df
            return df.filter(cond)
        case Map(input=inp, new_column=col, expression=expression, enabled=enabled):
            df = dataframes[inp]
            if not enabled:
                return df
            return df.withColumn(col, expr(expression))
        case Reduce(input=inp, group_by=group, aggregation=agg, enabled=enabled):
            df = dataframes[inp]
            if not enabled:
                return df
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


def execute_pipeline_with_session(spark: SparkSession, pipeline: Pipeline) -> None:
    dataframes: dict[str, DataFrame] = {}
    # source
    for source in pipeline.sources:
        df = spark.read.format(source.format).options(**source.options).load(source.path)
        dataframes[source.name] = df
    # transform
    apply_transformations(pipeline, dataframes)
    # sink
    for sink in pipeline.sinks:
        df = dataframes[pipeline.sink.input]
        df.write.mode("overwrite").format(sink.format).options(**sink.options).save(sink.path)


def execute_pipeline(spark_master: str | None, job_id: str | None, pipeline: Pipeline) -> None:
    spark: SparkSession | None = None
    if spark_master is None:
        spark_master = "local[*]"
    if job_id is None:
        job_id = str(uuid.uuid4())
    try:
        spark = (SparkSession.builder
                 .appName(job_id)
                 .master(spark_master)
                 .getOrCreate())
        execute_pipeline_with_session(spark, pipeline)
    finally:
        if spark is not None:
            spark.stop()
