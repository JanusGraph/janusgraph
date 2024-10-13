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

import org.janusgraph.core.schema.ConsistencyModifier;

import java.util.List;

public class JsonCompositeIndexDefinition extends AbstractJsonGraphCentricIndexDefinition {

    private ConsistencyModifier consistency;

    private Boolean unique;

    private List<String> inlinePropertyKeys;

    public ConsistencyModifier getConsistency() {
        return consistency;
    }

    public void setConsistency(ConsistencyModifier consistency) {
        this.consistency = consistency;
    }

    public Boolean getUnique() {
        return unique;
    }

    public void setUnique(Boolean unique) {
        this.unique = unique;
    }

    public List<String> getInlinePropertyKeys() {
        return inlinePropertyKeys;
    }

    public void setInlinePropertyKeys(List<String> inlinePropertyKeys) {
        this.inlinePropertyKeys = inlinePropertyKeys;
    }
}
