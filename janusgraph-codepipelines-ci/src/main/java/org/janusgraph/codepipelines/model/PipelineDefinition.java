package org.janusgraph.codepipelines.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;

@Value
public class PipelineDefinition {
    private final String name;
    private final List<ParallelBuildAction> parallelBuildActions;
    @JsonCreator
    public PipelineDefinition(@JsonProperty("name") String name, @JsonProperty("parallelBuildActions") List<ParallelBuildAction> parallelBuildActions) {
        this.name = name;
        this.parallelBuildActions = parallelBuildActions;
    }
}
