// Copyright 2024 JanusGraph Authors
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

package org.janusgraph.core.schema;

import org.janusgraph.graphdb.configuration.ConfigName;

public enum IndicesActivationType implements ConfigName {
    SKIP_ACTIVATION("skip_activation"),
    REINDEX_AND_ENABLE_UPDATED_ONLY("reindex_and_enable_updated_only"),
    REINDEX_AND_ENABLE_NON_ENABLED("reindex_and_enable_non_enabled"),
    FORCE_ENABLE_UPDATED_ONLY("force_enable_updated_only"),
    FORCE_ENABLE_NON_ENABLED("force_enable_non_enabled");

    private final String configurationOptionName;

    IndicesActivationType(String configurationOptionName){
        this.configurationOptionName = configurationOptionName;
    }

    @Override
    public String getConfigName() {
        return configurationOptionName;
    }
}
