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

package org.janusgraph.core.schema.json.definition.index;

import org.janusgraph.core.schema.json.definition.JsonParameterDefinition;

import java.util.List;

public class JsonIndexedPropertyKeyDefinition {

    private String propertyKey;

    private List<JsonParameterDefinition> parameters;

    public String getPropertyKey() {
        return propertyKey;
    }

    public void setPropertyKey(String propertyKey) {
        this.propertyKey = propertyKey;
    }

    public List<JsonParameterDefinition> getParameters() {
        return parameters;
    }

    public void setParameters(List<JsonParameterDefinition> parameters) {
        this.parameters = parameters;
    }
}
