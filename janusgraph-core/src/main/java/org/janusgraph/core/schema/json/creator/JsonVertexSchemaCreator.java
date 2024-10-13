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

package org.janusgraph.core.schema.json.creator;

import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.VertexLabelMaker;
import org.janusgraph.core.schema.json.definition.JsonVertexLabelDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class JsonVertexSchemaCreator implements JsonSchemaCreator<JsonVertexLabelDefinition> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonVertexSchemaCreator.class);

    @Override
    public boolean create(JsonVertexLabelDefinition definition, JsonSchemaCreationContext context) {

        JanusGraphManagement graphManagement = context.getGraphManagement();

        if(isVertexExists(graphManagement, definition)){
            LOGGER.info("Creation of the vertex {} was skipped because it is already exists.", definition.getLabel());
            return false;
        }

        VertexLabel vertexLabel = createVertex(graphManagement, definition);
        if(definition.getTtl() != null){
            graphManagement.setTTL(vertexLabel, Duration.ofMillis(definition.getTtl()));
        }

        LOGGER.info("Vertex {} was created", definition.getLabel());

        return true;
    }

    private VertexLabel createVertex(JanusGraphManagement graphManagement,
                                     JsonVertexLabelDefinition definition) {

        VertexLabelMaker vertexLabelMaker = graphManagement.makeVertexLabel(definition.getLabel());

        if(Boolean.TRUE.equals(definition.getPartition())) {
            vertexLabelMaker.partition();
        }

        if(Boolean.TRUE.equals(definition.getStaticVertex())) {
            vertexLabelMaker.setStatic();
        }

        return vertexLabelMaker.make();
    }

    private boolean isVertexExists(JanusGraphManagement graphManagement,
                                   JsonVertexLabelDefinition definition) {
        return graphManagement.containsVertexLabel(definition.getLabel());
    }

}
