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

import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.schema.EdgeLabelMaker;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.json.definition.JsonEdgeLabelDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class JsonEdgeSchemaCreator implements JsonSchemaCreator<JsonEdgeLabelDefinition> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonEdgeSchemaCreator.class);

    @Override
    public boolean create(JsonEdgeLabelDefinition definition, JsonSchemaCreationContext context) {

        JanusGraphManagement graphManagement = context.getGraphManagement();

        if(isEdgeExists(graphManagement, definition)) {
            LOGGER.info("Creation of the edge {} was skipped because it is already exists.", definition.getLabel());
            return false;
        }

        EdgeLabel edgeLabel = createEdge(graphManagement, definition);
        if(definition.getTtl() != null){
            graphManagement.setTTL(edgeLabel, Duration.ofMillis(definition.getTtl()));
        }
        if(definition.getConsistency() != null){
            graphManagement.setConsistency(edgeLabel, definition.getConsistency());
        }

        LOGGER.info("Edge {} was created", definition.getLabel());

        return true;

    }

    private EdgeLabel createEdge(JanusGraphManagement graphManagement,
                                 JsonEdgeLabelDefinition definition) {

        EdgeLabelMaker edgeLabelMaker = graphManagement.makeEdgeLabel(definition.getLabel());

        if(Boolean.TRUE.equals(definition.getUnidirected())) {
            edgeLabelMaker.unidirected();
        } else {
            edgeLabelMaker.directed();
        }

        if(definition.getMultiplicity() != null){
            edgeLabelMaker.multiplicity(definition.getMultiplicity());
        }

        return edgeLabelMaker.make();
    }

    private boolean isEdgeExists(JanusGraphManagement graphManagement,
                                 JsonEdgeLabelDefinition definition) {
        return graphManagement.containsEdgeLabel(definition.getLabel());
    }
}
