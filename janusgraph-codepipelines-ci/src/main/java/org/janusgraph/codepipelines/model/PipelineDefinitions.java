package org.janusgraph.codepipelines.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;

@Value
public class PipelineDefinitions {
    private final List<PipelineDefinition> pipelines;
    @JsonCreator
    public PipelineDefinitions(@JsonProperty("pipelines") List<PipelineDefinition> pipelines) {
        this.pipelines = pipelines;
    }
}
