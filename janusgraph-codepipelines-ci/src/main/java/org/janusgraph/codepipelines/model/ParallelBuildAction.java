package org.janusgraph.codepipelines.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;

@Value
public class ParallelBuildAction {
    private final String name;
    private final List<EnvironmentMapping> env;
    @JsonCreator
    public ParallelBuildAction(@JsonProperty("name") String name, @JsonProperty("env") List<EnvironmentMapping> env) {
        this.name = name;
        this.env = env;
    }
}
