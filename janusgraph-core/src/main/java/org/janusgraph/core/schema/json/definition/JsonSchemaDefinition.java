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

import org.janusgraph.core.schema.json.definition.index.JsonCompositeIndexDefinition;
import org.janusgraph.core.schema.json.definition.index.JsonMixedIndexDefinition;
import org.janusgraph.core.schema.json.definition.index.JsonVertexCentricEdgeIndexDefinition;
import org.janusgraph.core.schema.json.definition.index.JsonVertexCentricPropertyIndexDefinition;

import java.util.List;

public class JsonSchemaDefinition {

    private List<JsonVertexLabelDefinition> vertexLabels;
    private List<JsonEdgeLabelDefinition> edgeLabels;
    private List<JsonPropertyKeyDefinition> propertyKeys;
    private List<JsonCompositeIndexDefinition> compositeIndexes;
    private List<JsonVertexCentricEdgeIndexDefinition> vertexCentricEdgeIndexes;
    private List<JsonVertexCentricPropertyIndexDefinition> vertexCentricPropertyIndexes;
    private List<JsonMixedIndexDefinition> mixedIndexes;

    public List<JsonVertexLabelDefinition> getVertexLabels() {
        return vertexLabels;
    }

    public void setVertexLabels(List<JsonVertexLabelDefinition> vertexLabels) {
        this.vertexLabels = vertexLabels;
    }

    public List<JsonEdgeLabelDefinition> getEdgeLabels() {
        return edgeLabels;
    }

    public void setEdgeLabels(List<JsonEdgeLabelDefinition> edgeLabels) {
        this.edgeLabels = edgeLabels;
    }

    public List<JsonPropertyKeyDefinition> getPropertyKeys() {
        return propertyKeys;
    }

    public void setPropertyKeys(List<JsonPropertyKeyDefinition> propertyKeys) {
        this.propertyKeys = propertyKeys;
    }

    public List<JsonCompositeIndexDefinition> getCompositeIndexes() {
        return compositeIndexes;
    }

    public void setCompositeIndexes(List<JsonCompositeIndexDefinition> compositeIndexes) {
        this.compositeIndexes = compositeIndexes;
    }

    public List<JsonVertexCentricEdgeIndexDefinition> getVertexCentricEdgeIndexes() {
        return vertexCentricEdgeIndexes;
    }

    public void setVertexCentricEdgeIndexes(List<JsonVertexCentricEdgeIndexDefinition> vertexCentricEdgeIndexes) {
        this.vertexCentricEdgeIndexes = vertexCentricEdgeIndexes;
    }

    public List<JsonVertexCentricPropertyIndexDefinition> getVertexCentricPropertyIndexes() {
        return vertexCentricPropertyIndexes;
    }

    public void setVertexCentricPropertyIndexes(List<JsonVertexCentricPropertyIndexDefinition> vertexCentricPropertyIndexes) {
        this.vertexCentricPropertyIndexes = vertexCentricPropertyIndexes;
    }

    public List<JsonMixedIndexDefinition> getMixedIndexes() {
        return mixedIndexes;
    }

    public void setMixedIndexes(List<JsonMixedIndexDefinition> mixedIndexes) {
        this.mixedIndexes = mixedIndexes;
    }
}
