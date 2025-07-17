from mini_pipeline.domain.core.types import *


class Dataset(BaseModel):
    name: str


class PipelineTemplate(BaseModel):
    sources: List[Dataset]
    sinks: List[Dataset]
    transformations: List[TransformationType]

    def to_pipeline(self, sources: List[Source], sinks: List[Sink]) -> Pipeline:
        source_dict = {source.name: source for source in sources}
        sink_dict = {sink.input: sink for sink in sinks}

        template_source_names = {ds.name for ds in self.sources}
        template_sink_names = {ds.name for ds in self.sinks}

        missing_sources = template_source_names - source_dict.keys()
        missing_sinks = template_sink_names - sink_dict.keys()

        if missing_sources:
            raise ValueError(f"Missing sources in input: {missing_sources}")
        if missing_sinks:
            raise ValueError(f"Missing sinks in input: {missing_sinks}")

        return Pipeline(
            sources=[source_dict[name] for name in template_source_names],
            sinks=[sink_dict[name] for name in template_sink_names],
            transformations=self.transformations
        )
