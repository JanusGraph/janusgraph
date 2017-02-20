package org.janusgraph.codepipelines.model;

import com.fasterxml.jackson.annotation.JsonCreator;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;

import java.util.List;

@Value
public class EnvironmentMapping {
    private final String name;
    private final String value;
    @JsonCreator
    public EnvironmentMapping(@JsonProperty("name") String name, @JsonProperty("value") String value) {
        this.name = name;
        this.value = value;
    }
}
