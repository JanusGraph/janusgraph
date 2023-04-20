// Copyright 2023 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.optimize.strategy;

import org.janusgraph.graphdb.configuration.ConfigName;

public enum MultiQueryHasStepStrategyMode implements ConfigName {

    /**
     * Prefetch all properties on any property access.
     */
    ALL_PROPERTIES("all_properties"),

    /**
     * Prefetch properties required for has step evaluation only.
     * Notice, multiple `has` steps which are folded into a single has step may have multiple properties.
     * All of those folded HasContainers will be prefetched together in the same multi-query.
     */
    REQUIRED_PROPERTIES_ONLY("required_properties_only"),

    /**
     * Prefetch the same properties as with `REQUIRED_PROPERTIES_ONLY` mode, but also prefetch
     * properties which may be needed in the next properties access step like `values`, `properties,` `valueMap`, or `elementMap`.
     * In case the next step is not one of those properties access steps then this mode behaves same as `REQUIRED_PROPERTIES_ONLY`.
     * In case the next step is one of the properties access steps with limited scope of properties, those properties will be
     * pre-fetched together in the same multi-query.
     * In case the next step is one of the properties access steps with unlimited scope of properties then this mode
     * behaves same as `ALL_PROPERTIES`.
     */
    REQUIRED_AND_NEXT_PROPERTIES("required_and_next_properties"),

    /**
     * Prefetch the same properties as with `REQUIRED_AND_NEXT_PROPERTIES`, but in case the next step is not
     * `values`, `properties,` `valueMap`, or `elementMap` then acts like `ALL_PROPERTIES`.
     */
    REQUIRED_AND_NEXT_PROPERTIES_OR_ALL("required_and_next_properties_or_all"),

    /**
     * Skips `has` step pre-fetch optimization.
     */
    NONE("none")
    ;

    private final String configurationOptionName;

    MultiQueryHasStepStrategyMode(String configurationOptionName){
        this.configurationOptionName = configurationOptionName;
    }

    @Override
    public String getConfigName() {
        return configurationOptionName;
    }
}
