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

import org.apache.commons.lang3.StringUtils;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.Namifiable;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.schema.json.creator.GeneralJsonSchemaCreator;
import org.janusgraph.core.schema.json.creator.JsonSchemaCreationContext;
import org.janusgraph.core.schema.json.creator.SchemaCreationException;
import org.janusgraph.core.schema.json.definition.JsonSchemaDefinition;
import org.janusgraph.core.schema.json.definition.index.AbstractJsonGraphCentricIndexDefinition;
import org.janusgraph.core.schema.json.definition.index.AbstractJsonIndexDefinition;
import org.janusgraph.core.schema.json.definition.index.AbstractJsonVertexCentricIndexDefinition;
import org.janusgraph.core.schema.json.definition.index.JsonVertexCentricEdgeIndexDefinition;
import org.janusgraph.core.schema.json.definition.index.JsonVertexCentricPropertyIndexDefinition;
import org.janusgraph.core.util.JsonUtil;
import org.janusgraph.core.util.ManagementUtil;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JsonSchemaInitStrategy implements SchemaInitStrategy{

    private static final Logger LOG = LoggerFactory.getLogger(JsonSchemaInitStrategy.class);

    @Override
    public JanusGraph initializeSchemaAndStart(GraphDatabaseConfiguration graphDatabaseConfiguration) {

        JanusGraph graph = new StandardJanusGraph(graphDatabaseConfiguration);

        try{
            if(StringUtils.isNotEmpty(graphDatabaseConfiguration.getSchemaInitJsonString())) {

                initializeSchemaFromString(graph, graphDatabaseConfiguration.getSchemaInitJsonString());

            } else if(StringUtils.isNotEmpty(graphDatabaseConfiguration.getSchemaInitJsonFile())){

                initializeSchemaFromFile(graph, graphDatabaseConfiguration.getSchemaInitJsonFile());

            } else {
                throw new SchemaCreationException("When JSON schema initialization is enabled it's necessary to provide either JSON file or string for schema initialization.");
            }
        } catch (Throwable throwable){
            graph.close();
            if(throwable instanceof SchemaCreationException){
                throw (SchemaCreationException)throwable;
            }
            throw new SchemaCreationException(throwable);
        }

        return graph;
    }

    public static void initializeSchemaFromFile(JanusGraph graph, String jsonSchemaFilePath) {
        GraphDatabaseConfiguration graphDatabaseConfiguration = ((StandardJanusGraph) graph).getConfiguration();
        IndicesActivationType indicesActivationType = graphDatabaseConfiguration.getSchemaInitJsonIndicesActivationType();
        boolean createSchemaElements = !graphDatabaseConfiguration.getSchemaInitJsonSkipElements();
        boolean createSchemaIndices = !graphDatabaseConfiguration.getSchemaInitJsonSkipIndices();
        long indexStatusTimeout = graphDatabaseConfiguration.getSchemaInitJsonIndexStatusAwaitTimeout();

        initializeSchemaFromFile(graph, createSchemaElements, createSchemaIndices, indicesActivationType, true,
            graphDatabaseConfiguration.getSchemaInitJsonForceCloseOtherInstances(), indexStatusTimeout, jsonSchemaFilePath);
    }

    public static void initializeSchemaFromString(JanusGraph graph, String jsonSchemaString) {
        GraphDatabaseConfiguration graphDatabaseConfiguration = ((StandardJanusGraph) graph).getConfiguration();
        IndicesActivationType indicesActivationType = graphDatabaseConfiguration.getSchemaInitJsonIndicesActivationType();
        boolean createSchemaElements = !graphDatabaseConfiguration.getSchemaInitJsonSkipElements();
        boolean createSchemaIndices = !graphDatabaseConfiguration.getSchemaInitJsonSkipIndices();
        long indexStatusTimeout = graphDatabaseConfiguration.getSchemaInitJsonIndexStatusAwaitTimeout();

        initializeSchemaFromString(graph, createSchemaElements, createSchemaIndices, indicesActivationType, true,
            graphDatabaseConfiguration.getSchemaInitJsonForceCloseOtherInstances(), indexStatusTimeout, jsonSchemaString);
    }

    public static void initializeSchemaFromFile(JanusGraph graph, boolean createSchemaElements, boolean createSchemaIndices,
                                                IndicesActivationType indicesActivationType,
                                                boolean forceRollBackActiveTransactions, boolean forceCloseOtherInstances,
                                                long indexStatusTimeout, String jsonSchemaFilePath) {

        JsonSchemaDefinition generalDefinition;
        try {
            generalDefinition = JsonUtil.jsonFilePathToObject(jsonSchemaFilePath, JsonSchemaDefinition.class);
        } catch (IOException e) {
            throw new SchemaCreationException(e);
        }

        initializeSchema(graph, createSchemaElements, createSchemaIndices, indicesActivationType,
            forceRollBackActiveTransactions, forceCloseOtherInstances, indexStatusTimeout, generalDefinition);
    }

    public static void initializeSchemaFromString(JanusGraph graph, boolean createSchemaElements, boolean createSchemaIndices,
                                                  IndicesActivationType indicesActivationType,
                                                  boolean forceRollBackActiveTransactions, boolean forceCloseOtherInstances,
                                                  long indexStatusTimeout, String jsonSchemaString) {

        JsonSchemaDefinition generalDefinition;
        try {
            generalDefinition = JsonUtil.jsonStringToObject(jsonSchemaString, JsonSchemaDefinition.class);
        } catch (IOException e) {
            throw new SchemaCreationException(e);
        }

        initializeSchema(graph, createSchemaElements, createSchemaIndices, indicesActivationType,
            forceRollBackActiveTransactions, forceCloseOtherInstances, indexStatusTimeout, generalDefinition);
    }

    public static void initializeSchema(JanusGraph graph, boolean createSchemaElements, boolean createSchemaIndices,
                                        IndicesActivationType indicesActivationType,
                                        boolean forceRollBackActiveTransactions, boolean forceCloseOtherInstances,
                                        long indexStatusTimeout, JsonSchemaDefinition generalDefinition) {

        LOG.info("Starting schema initialization.");

        if(forceCloseOtherInstances){
            ManagementUtil.forceCloseOtherInstances(graph);
        }

        if(forceRollBackActiveTransactions){
            ManagementUtil.forceRollbackAllTransactions(graph);
        }

        final ManagementSystem schemaCreationMgmt = (ManagementSystem) graph.openManagement();
        JsonSchemaCreationContext schemaCreationContext = new JsonSchemaCreationContext(
            schemaCreationMgmt,
            createSchemaElements,
            createSchemaIndices);

        GeneralJsonSchemaCreator generalJsonSchemaCreator = new GeneralJsonSchemaCreator();

        // initialize schema
        generalJsonSchemaCreator.create(generalDefinition, schemaCreationContext);

        List<String> updatedButNonEnabledIndicesNames = schemaCreationContext.getCreatedOrUpdatedIndices().stream()
            .filter(indexDefinition -> !isIndexEnabled(indexDefinition, schemaCreationMgmt))
            .map(AbstractJsonIndexDefinition::getName)
            .collect(Collectors.toList());

        schemaCreationMgmt.commit();

        // process indices activation (re-index and enable)
        processIndicesActivation(graph, updatedButNonEnabledIndicesNames, indicesActivationType, indexStatusTimeout);

        LOG.info("Schema initialization completed.");
    }

    private static void processIndicesActivation(JanusGraph graph, List<String> updatedButNonEnabledIndicesNames,
                                                 IndicesActivationType indicesActivationType, long indexStatusTimeout){

        if(IndicesActivationType.SKIP_ACTIVATION.equals(indicesActivationType)){
            return;
        }

        ManagementSystem schemaFetchMgmt = (ManagementSystem) graph.openManagement();

        List<JanusGraphIndex> nonEnabledGraphIndices = schemaFetchMgmt.getGraphIndices(SchemaStatus.ENABLED);
        List<RelationTypeIndex> nonEnabledRelationTypeIndices = schemaFetchMgmt.getVertexCentricIndices(SchemaStatus.ENABLED);

        if(IndicesActivationType.REINDEX_AND_ENABLE_UPDATED_ONLY.equals(indicesActivationType) || IndicesActivationType.FORCE_ENABLE_UPDATED_ONLY.equals(indicesActivationType)){
            List<JanusGraphIndex> nonEnabledGraphIndicesCopy = nonEnabledGraphIndices;
            List<RelationTypeIndex> nonEnabledRelationTypeIndicesCopy = nonEnabledRelationTypeIndices;
            nonEnabledGraphIndices = new ArrayList<>(nonEnabledGraphIndices.size());
            nonEnabledRelationTypeIndices = new ArrayList<>(nonEnabledRelationTypeIndices.size());
            for (JanusGraphIndex index : nonEnabledGraphIndicesCopy) {
                if(updatedButNonEnabledIndicesNames.contains(index.name())){
                    nonEnabledGraphIndices.add(index);
                }
            }
            for (RelationTypeIndex index : nonEnabledRelationTypeIndicesCopy) {
                if(updatedButNonEnabledIndicesNames.contains(index.name())){
                    nonEnabledRelationTypeIndices.add(index);
                }
            }
        }

        List<String> nonEnabledGraphIndexNames = nonEnabledGraphIndices.stream().map(JanusGraphIndex::name).collect(Collectors.toList());
        Map<String, String> nonEnabledRelationTypeIndexNamesAndTypes = nonEnabledRelationTypeIndices.stream().collect(Collectors.toMap(Namifiable::name, o -> o.getType().name()));

        schemaFetchMgmt.rollback();

        List<String> nonEnabledIndexNames = Stream.concat(nonEnabledGraphIndexNames.stream(), nonEnabledRelationTypeIndexNamesAndTypes.keySet().stream()).collect(Collectors.toList());

        if(!nonEnabledIndexNames.isEmpty()){

            String nonEnabledIndexNamesJoined = String.join(", ", nonEnabledIndexNames);
            if(!nonEnabledIndexNames.isEmpty() && LOG.isInfoEnabled()){
                LOG.info("Found {} non-enabled indices. Awaiting for their status updates. Indexes [{}].",
                    nonEnabledIndexNames.size(), nonEnabledIndexNamesJoined);
            }

            if(IndicesActivationType.REINDEX_AND_ENABLE_UPDATED_ONLY.equals(indicesActivationType) || IndicesActivationType.REINDEX_AND_ENABLE_NON_ENABLED.equals(indicesActivationType)){

                ManagementUtil.reindexAndEnableIndices(graph, nonEnabledGraphIndexNames, nonEnabledRelationTypeIndexNamesAndTypes, indexStatusTimeout);

            } else if(IndicesActivationType.FORCE_ENABLE_UPDATED_ONLY.equals(indicesActivationType) || IndicesActivationType.FORCE_ENABLE_NON_ENABLED.equals(indicesActivationType)){

                ManagementUtil.forceEnableIndices(graph, nonEnabledGraphIndexNames, nonEnabledRelationTypeIndexNamesAndTypes, indexStatusTimeout);

            } else {
                throw new IllegalStateException("Unknown indicesActivationType: " + indicesActivationType);
            }
        }
    }

    private static boolean isIndexEnabled(AbstractJsonIndexDefinition createdIndexesDefinition, JanusGraphManagement graphManagement){
        return isIndexHasStatus(createdIndexesDefinition, graphManagement, SchemaStatus.ENABLED);
    }

    private static boolean isIndexHasStatus(AbstractJsonIndexDefinition createdIndexesDefinition, JanusGraphManagement graphManagement, SchemaStatus schemaStatus){
        if(createdIndexesDefinition instanceof AbstractJsonVertexCentricIndexDefinition){

            AbstractJsonVertexCentricIndexDefinition vertexCentricIndex =
                (AbstractJsonVertexCentricIndexDefinition) createdIndexesDefinition;
            String indexedElement;
            if(vertexCentricIndex instanceof JsonVertexCentricEdgeIndexDefinition){
                indexedElement = ((JsonVertexCentricEdgeIndexDefinition) vertexCentricIndex).getIndexedEdgeLabel();
            } else if (vertexCentricIndex instanceof JsonVertexCentricPropertyIndexDefinition){
                indexedElement = ((JsonVertexCentricPropertyIndexDefinition) vertexCentricIndex).getIndexedPropertyKey();
            } else {
                throw new SchemaCreationException("Unknown index type definition: "+createdIndexesDefinition.getClass().getName());
            }
            RelationType relationType = graphManagement.getRelationType(indexedElement);
            RelationTypeIndex relationIndex = graphManagement.getRelationIndex(relationType, vertexCentricIndex.getName());
            return schemaStatus.equals(relationIndex.getIndexStatus());

        } else if (createdIndexesDefinition instanceof AbstractJsonGraphCentricIndexDefinition) {

            JanusGraphIndex janusGraphIndex = graphManagement.getGraphIndex(createdIndexesDefinition.getName());
            for(PropertyKey propertyKey : janusGraphIndex.getFieldKeys()){
                if(!schemaStatus.equals(janusGraphIndex.getIndexStatus(propertyKey))){
                    return false;
                }
            }
            return true;

        } else {
            throw new SchemaCreationException("Unknown index type definition: "+createdIndexesDefinition.getClass().getName());
        }
    }

}
