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

public enum SchemaInitType implements ConfigName {
        NONE("none"), // skips any schema initialization during startup
        JSON("json"); // initializes schema using provided JSON file

    private final String configurationOptionName;

    SchemaInitType(String configurationOptionName){
        this.configurationOptionName = configurationOptionName;
    }

    @Override
    public String getConfigName() {
        return configurationOptionName;
    }
}
