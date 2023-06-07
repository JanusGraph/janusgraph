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

public enum MultiQueryPropertiesStrategyMode implements ConfigName {

    /**
     * Prefetch all properties on any property access.
     */
    ALL_PROPERTIES("all_properties"),

    /**
     * Prefetch needed properties only.
     */
    REQUIRED_PROPERTIES_ONLY("required_properties_only"),

    /**
     * Skips properties pre-fetch optimization.
     */
    NONE("none")
    ;

    private final String configurationOptionName;

    MultiQueryPropertiesStrategyMode(String configurationOptionName){
        this.configurationOptionName = configurationOptionName;
    }

    @Override
    public String getConfigName() {
        return configurationOptionName;
    }
}
