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

package org.janusgraph.core.schema.json.definition;

import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.schema.ConsistencyModifier;

public class JsonEdgeLabelDefinition {

    private String label;

    private Multiplicity multiplicity;

    private Boolean unidirected;

    private Long ttl;

    private ConsistencyModifier consistency;

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Multiplicity getMultiplicity() {
        return multiplicity;
    }

    public void setMultiplicity(Multiplicity multiplicity) {
        this.multiplicity = multiplicity;
    }

    public Boolean getUnidirected() {
        return unidirected;
    }

    public void setUnidirected(Boolean unidirected) {
        this.unidirected = unidirected;
    }

    public Long getTtl() {
        return ttl;
    }

    public void setTtl(Long ttl) {
        this.ttl = ttl;
    }

    public ConsistencyModifier getConsistency() {
        return consistency;
    }

    public void setConsistency(ConsistencyModifier consistency) {
        this.consistency = consistency;
    }
}
