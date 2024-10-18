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

public class JsonVertexLabelDefinition {

    private String label;

    private Boolean staticVertex;

    private Boolean partition;

    private Long ttl;

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Boolean getStaticVertex() {
        return staticVertex;
    }

    public void setStaticVertex(Boolean staticVertex) {
        this.staticVertex = staticVertex;
    }

    public Boolean getPartition() {
        return partition;
    }

    public void setPartition(Boolean partition) {
        this.partition = partition;
    }

    public Long getTtl() {
        return ttl;
    }

    public void setTtl(Long ttl) {
        this.ttl = ttl;
    }
}
