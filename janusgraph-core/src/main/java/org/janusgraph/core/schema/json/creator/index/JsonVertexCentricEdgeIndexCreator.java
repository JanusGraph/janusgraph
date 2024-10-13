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

package org.janusgraph.core.schema.json.creator.index;

import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.Index;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.json.creator.JsonSchemaCreationContext;
import org.janusgraph.core.schema.json.definition.index.JsonVertexCentricEdgeIndexDefinition;

public class JsonVertexCentricEdgeIndexCreator extends AbstractJsonVertexCentricIndexCreator<JsonVertexCentricEdgeIndexDefinition> {

    @Override
    protected Index buildIndex(JsonVertexCentricEdgeIndexDefinition definition, JsonSchemaCreationContext context){
        JanusGraphManagement graphManagement = context.getGraphManagement();
        EdgeLabel edgeLabel = graphManagement.getEdgeLabel(definition.getIndexedEdgeLabel());
        PropertyKey[] propertyKeys = toPropertyKeys(definition, context);

        if (definition.getOrder() == null) {
            return graphManagement.buildEdgeIndex(
                edgeLabel,
                definition.getName(),
                definition.getDirection(),
                propertyKeys
            );
        } else {
            return graphManagement.buildEdgeIndex(
                edgeLabel,
                definition.getName(),
                definition.getDirection(),
                definition.getOrder(),
                propertyKeys
            );
        }
    }

    @Override
    protected String getIndexedElementName(JsonVertexCentricEdgeIndexDefinition definition) {
        return definition.getIndexedEdgeLabel();
    }
}
