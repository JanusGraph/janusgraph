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
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import lombok.Value;
import software.amazon.awssdk.services.codebuild.model.ComputeType;

@Value
public class ParallelBuildAction {
    private final String name;
    private final Optional<ComputeType> computeType;
    private final Optional<String> computeImage;
    private final Optional<Boolean> privilegedMode;
    private final int timeout;
    private final List<EnvironmentMapping> env;
    @JsonCreator
    public ParallelBuildAction(@JsonProperty("name") String name,
        @JsonProperty("computeType") String computeType,
        @JsonProperty("computeImage") String computeImage,
        @JsonProperty("privilegedMode") Boolean privilegedMode,
        @JsonProperty("env") List<EnvironmentMapping> env,
        @JsonProperty("timeout") int timeout) {
        this.name = name;
        this.computeType = Strings.isNullOrEmpty(computeType) ? Optional.empty() : Optional.of(ComputeType.fromValue(computeType));
        this.computeImage = Strings.isNullOrEmpty(computeImage) ? Optional.empty() : Optional.of(computeImage);
        this.privilegedMode = privilegedMode == null ? Optional.empty() : Optional.of(privilegedMode);
        this.timeout = timeout;
        this.env = env;
    }
}
