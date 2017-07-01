// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package org.janusgraph.codepipelines.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import software.amazon.awssdk.services.codebuild.model.ComputeType;

@Getter
public class PipelineDefinitions {
    private final List<PipelineDefinition> pipelines;
    private final String defaultComputeImage;
    private final ComputeType defaultComputeType;
    private final boolean defaultPrivilegedMode;
    @JsonCreator
    public PipelineDefinitions(@JsonProperty("pipelines") final List<PipelineDefinition> pipelines,
                               @JsonProperty("defaultComputeImage") final String defaultComputeImage,
                               @JsonProperty("defaultComputeType") final String defaultComputeType,
                               @JsonProperty("defaultPrivilegedMode") final boolean defaultPrivilegedMode) {
        this.pipelines = pipelines;
        this.defaultComputeImage = defaultComputeImage;
        this.defaultComputeType = ComputeType.fromValue(defaultComputeType);
        this.defaultPrivilegedMode = defaultPrivilegedMode;
    }
}
