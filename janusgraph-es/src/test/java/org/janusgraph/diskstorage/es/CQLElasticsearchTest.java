// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.diskstorage.es;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.JanusGraphCassandraContainer;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.schema.IndicesActivationType;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaInitType;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.core.schema.json.definition.JsonSchemaDefinition;
import org.janusgraph.core.util.JsonUtil;
import org.janusgraph.core.util.ManagementUtil;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.SCHEMA_DROP_BEFORE_INIT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.SCHEMA_INIT_JSON_FILE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.SCHEMA_INIT_JSON_FORCE_CLOSE_OTHER_INSTANCES;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.SCHEMA_INIT_JSON_INDICES_ACTIVATION_TYPE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.SCHEMA_INIT_JSON_SKIP_ELEMENTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.SCHEMA_INIT_JSON_SKIP_INDICES;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.SCHEMA_INIT_STRATEGY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class CQLElasticsearchTest extends ElasticsearchJanusGraphIndexTest {

    @Container
    private static JanusGraphCassandraContainer cql = new JanusGraphCassandraContainer();

    @Override
    public ModifiableConfiguration getStorageConfiguration() {
        return cql.getConfiguration(CQLElasticsearchTest.class.getName());
    }

    @Override
    @Disabled("CQL seems to not clear storage correctly")
    public void testClearStorage() {}

    @Test
    public void testJsonFreshSchemaImport() {
        String jsonFilePath = createJsonFileAndReturnPath();

        clopen(
            option(SCHEMA_INIT_STRATEGY), SchemaInitType.JSON.getConfigName(),
            option(SCHEMA_DROP_BEFORE_INIT), true,
            option(SCHEMA_INIT_JSON_FILE), jsonFilePath,
            option(SCHEMA_INIT_JSON_INDICES_ACTIVATION_TYPE), IndicesActivationType.REINDEX_AND_ENABLE_UPDATED_ONLY.getConfigName()
        );
        assertSchemaElements();
        assertIndices(SchemaStatus.ENABLED);

        // Check additional run doesn't change anything
        clopen(
            option(SCHEMA_INIT_STRATEGY), SchemaInitType.JSON.getConfigName(),
            option(SCHEMA_DROP_BEFORE_INIT), false,
            option(SCHEMA_INIT_JSON_FILE), jsonFilePath
        );
        assertSchemaElements();
        assertIndices(SchemaStatus.ENABLED);

        // Check schema from documentation can be initialized
        String jsonFromDocFilePath = createJsonFileAndReturnPath("test_schema_from_doc.json");
        clopen(
            option(SCHEMA_INIT_STRATEGY), SchemaInitType.JSON.getConfigName(),
            option(SCHEMA_DROP_BEFORE_INIT), true,
            option(SCHEMA_INIT_JSON_FILE), jsonFromDocFilePath,
            option(SCHEMA_INIT_JSON_INDICES_ACTIVATION_TYPE), IndicesActivationType.REINDEX_AND_ENABLE_NON_ENABLED.getConfigName()
        );
    }

    @Test
    public void testJsonGradualSchemaImport() {
        String jsonFilePath = createJsonFileAndReturnPath();

        clopen(
            option(SCHEMA_INIT_STRATEGY), SchemaInitType.JSON.getConfigName(),
            option(SCHEMA_DROP_BEFORE_INIT), true,
            option(SCHEMA_INIT_JSON_FILE), jsonFilePath,
            option(SCHEMA_INIT_JSON_INDICES_ACTIVATION_TYPE), IndicesActivationType.REINDEX_AND_ENABLE_NON_ENABLED.getConfigName(),
            option(SCHEMA_INIT_JSON_SKIP_ELEMENTS), false,
            option(SCHEMA_INIT_JSON_SKIP_INDICES), true
        );

        createData();
        assertDataFetch(false, true);

        clopen(
            option(SCHEMA_INIT_STRATEGY), SchemaInitType.JSON.getConfigName(),
            option(SCHEMA_DROP_BEFORE_INIT), false,
            option(SCHEMA_INIT_JSON_FILE), jsonFilePath,
            option(SCHEMA_INIT_JSON_INDICES_ACTIVATION_TYPE), IndicesActivationType.REINDEX_AND_ENABLE_UPDATED_ONLY.getConfigName(),
            option(SCHEMA_INIT_JSON_SKIP_ELEMENTS), true,
            option(SCHEMA_INIT_JSON_SKIP_INDICES), false
        );

        assertSchemaElements();
        assertIndices(SchemaStatus.ENABLED);
        assertDataFetch(true, true);
    }

    @Test
    public void testJsonForceSchemaImport() {
        String jsonFilePath = createJsonFileAndReturnPath();

        clopen(
            option(SCHEMA_INIT_STRATEGY), SchemaInitType.JSON.getConfigName(),
            option(SCHEMA_DROP_BEFORE_INIT), true,
            option(SCHEMA_INIT_JSON_FILE), jsonFilePath,
            option(SCHEMA_INIT_JSON_INDICES_ACTIVATION_TYPE), IndicesActivationType.FORCE_ENABLE_NON_ENABLED.getConfigName(),
            option(SCHEMA_INIT_JSON_SKIP_ELEMENTS), false,
            option(SCHEMA_INIT_JSON_SKIP_INDICES), true
        );

        createData();
        assertDataFetch(false, true);

        clopen(
            option(SCHEMA_INIT_STRATEGY), SchemaInitType.JSON.getConfigName(),
            option(SCHEMA_DROP_BEFORE_INIT), false,
            option(SCHEMA_INIT_JSON_FILE), jsonFilePath,
            option(SCHEMA_INIT_JSON_INDICES_ACTIVATION_TYPE), IndicesActivationType.FORCE_ENABLE_UPDATED_ONLY.getConfigName(),
            option(SCHEMA_INIT_JSON_SKIP_ELEMENTS), true,
            option(SCHEMA_INIT_JSON_SKIP_INDICES), false
        );

        assertSchemaElements();
        assertIndices(SchemaStatus.ENABLED);
        assertDataFetch(true, false);
    }

    @Test
    public void testJsonSchemaImportSkipIndicesActivation() {
        String jsonFilePath = createJsonFileAndReturnPath();

        clopen(
            option(SCHEMA_INIT_STRATEGY), SchemaInitType.JSON.getConfigName(),
            option(SCHEMA_DROP_BEFORE_INIT), true,
            option(SCHEMA_INIT_JSON_FILE), jsonFilePath,
            option(SCHEMA_INIT_JSON_INDICES_ACTIVATION_TYPE), IndicesActivationType.FORCE_ENABLE_NON_ENABLED.getConfigName(),
            option(SCHEMA_INIT_JSON_SKIP_ELEMENTS), false,
            option(SCHEMA_INIT_JSON_SKIP_INDICES), true
        );

        createData();
        assertDataFetch(false, true);

        clopen(
            option(SCHEMA_INIT_STRATEGY), SchemaInitType.JSON.getConfigName(),
            option(SCHEMA_DROP_BEFORE_INIT), false,
            option(SCHEMA_INIT_JSON_FILE), jsonFilePath,
            option(SCHEMA_INIT_JSON_INDICES_ACTIVATION_TYPE), IndicesActivationType.SKIP_ACTIVATION.getConfigName(),
            option(SCHEMA_INIT_JSON_SKIP_ELEMENTS), true,
            option(SCHEMA_INIT_JSON_SKIP_INDICES), false
        );

        assertSchemaElements();
        assertIndices(SchemaStatus.INSTALLED);
        assertDataFetch(false, true);
    }

    @Test
    public void testJsonGradualSchemaImportForceClosingOtherInstances() {
        String jsonFilePath = createJsonFileAndReturnPath();

        clopen(
            option(SCHEMA_INIT_STRATEGY), SchemaInitType.JSON.getConfigName(),
            option(SCHEMA_DROP_BEFORE_INIT), true,
            option(SCHEMA_INIT_JSON_FILE), jsonFilePath,
            option(SCHEMA_INIT_JSON_INDICES_ACTIVATION_TYPE), IndicesActivationType.REINDEX_AND_ENABLE_NON_ENABLED.getConfigName(),
            option(SCHEMA_INIT_JSON_SKIP_ELEMENTS), false,
            option(SCHEMA_INIT_JSON_SKIP_INDICES), true,
            option(UNIQUE_INSTANCE_ID), "graph1"

        );

        createData();
        assertDataFetch(false, true);

        setupConfig(option(SCHEMA_INIT_STRATEGY), SchemaInitType.JSON.getConfigName(),
            option(SCHEMA_DROP_BEFORE_INIT), false,
            option(SCHEMA_INIT_JSON_FILE), jsonFilePath,
            option(SCHEMA_INIT_JSON_INDICES_ACTIVATION_TYPE), IndicesActivationType.REINDEX_AND_ENABLE_UPDATED_ONLY.getConfigName(),
            option(SCHEMA_INIT_JSON_SKIP_ELEMENTS), true,
            option(SCHEMA_INIT_JSON_SKIP_INDICES), false,
            option(SCHEMA_INIT_JSON_FORCE_CLOSE_OTHER_INSTANCES), true,
            option(UNIQUE_INSTANCE_ID), "graph2"
        );

        JanusGraph prevGraph = graph;
        JanusGraphManagement prevMgmt = mgmt;
        if(!prevMgmt.isOpen()){
            prevMgmt = graph.openManagement();
        }
        open(config);

        assertSchemaElements();
        assertIndices(SchemaStatus.ENABLED);
        assertDataFetch(true, true);

        if(prevMgmt.isOpen()){
            prevMgmt.rollback();
        }
        if(prevGraph.isOpen()){
            prevGraph.close();
        }
    }

    @Test
    public void testCustomSchemaInitStrategy() {
        assertFalse(CustomTestSchemaInitStrategy.initialized);
        clopen(
            option(SCHEMA_INIT_STRATEGY), CustomTestSchemaInitStrategy.class.getName()
        );
        assertTrue(CustomTestSchemaInitStrategy.initialized);
    }

    @Test
    public void testJsonSchemaResourceParseToObject() throws IOException {
        JsonSchemaDefinition jsonSchemaDefinition = JsonUtil
            .jsonResourcePathToObject("test_schema_example.json", JsonSchemaDefinition.class);
        assertNotNull(jsonSchemaDefinition);
    }

    private void assertDataFetch(boolean indicesUsed, boolean indexedDataShouldBeFound){
        GraphTraversalSource g = graph.traversal();

        try {
            Callable<GraphTraversal<?,?>> query = () -> g.V().hasLabel("organization").has("name", "test_org1");
            assertEquals(indicesUsed, query.call().profile().next().toString().contains("index="));
            assertEquals(indexedDataShouldBeFound, query.call().hasNext());

            query = () -> g.V().hasLabel("organization").has("name", "test_org2");
            assertEquals(indicesUsed, query.call().profile().next().toString().contains("index="));
            assertEquals(indexedDataShouldBeFound, query.call().hasNext());

            query = () -> g.V().hasLabel("device").has("name", "test_org3");
            assertEquals(indicesUsed, query.call().profile().next().toString().contains("index="));
            assertEquals(indexedDataShouldBeFound, query.call().hasNext());

            if(indexedDataShouldBeFound){
                Vertex vertex = g.V().hasLabel("organization").has("name", "test_org1").next();
                query = () -> g.V(vertex).properties("longPropCardinalityList").has("time", P.lt(300));
                assertEquals(indicesUsed, query.call().profile().next().toString().contains("longPropCardinalityListMetaPropertyVertexCentricIndexForTime"));
                assertTrue(query.call().hasNext());
            }

            query = () -> g.E().hasLabel("connects").has("name", "connectsEdge1");
            assertEquals(indicesUsed, query.call().profile().next().toString().contains("index="));
            assertEquals(indexedDataShouldBeFound, query.call().hasNext());

            query = () -> g.E().hasLabel("viewed").has("name", "connectsEdge4");
            // No indices were defined for such query. Thus, it should always be `false`.
            assertFalse(query.call().profile().next().toString().contains("index="));
            assertTrue(query.call().profile().next().toString().contains("fullscan=true"));
            assertTrue(query.call().hasNext());

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            g.tx().rollback();
        }
    }

    private void assertSchemaElements(){
        assertEquals(Long.class, mgmt.getPropertyKey("time").dataType());
        assertEquals(Double.class, mgmt.getPropertyKey("doubleProp").dataType());
        assertEquals(Cardinality.LIST, mgmt.getPropertyKey("longPropCardinalityList").cardinality());

        assertEquals("organization", mgmt.getVertexLabel("organization").name());

        assertEquals(Multiplicity.SIMPLE, mgmt.getEdgeLabel("connects").multiplicity());
        assertTrue(mgmt.getEdgeLabel("connects").isDirected());
        assertEquals(Multiplicity.MULTI, mgmt.getEdgeLabel("viewed").multiplicity());
        assertTrue(mgmt.getEdgeLabel("viewed").isUnidirected());
    }

    private void assertIndices(SchemaStatus status){
        assertTrue(ManagementUtil.isIndexHasStatus(mgmt.getGraphIndex("nameCompositeIndex"), status));
        assertTrue(ManagementUtil.isIndexHasStatus(mgmt.getGraphIndex("timeForOrganizationsOnlyCompositeIndex"), status));
        assertTrue(ManagementUtil.isIndexHasStatus(mgmt.getGraphIndex("connectsOnlyEdgeCompositeIndex"), status));
        assertTrue(ManagementUtil.isIndexHasStatus(mgmt.getGraphIndex("uniqueCompositeIndexWithLocking"), status));
        assertTrue(ManagementUtil.isIndexHasStatus(mgmt.getGraphIndex("multiKeysCompositeIndex"), status));
        assertTrue(ManagementUtil.isIndexHasStatus(mgmt.getGraphIndex("nameMixedIndex"), status));

        assertTrue(ManagementUtil.isIndexHasStatus(mgmt.getRelationIndex(mgmt.getRelationType("connects"), "connectsTimeVertexCentricIndex"), status));
        assertTrue(ManagementUtil.isIndexHasStatus(mgmt.getRelationIndex(mgmt.getRelationType("viewed"), "vertexCentricUnidirectedEdgeIndexWithTwoProps"), status));
        assertTrue(ManagementUtil.isIndexHasStatus(mgmt.getRelationIndex(mgmt.getRelationType("longPropCardinalityList"), "longPropCardinalityListMetaPropertyVertexCentricIndexForTime"), status));
    }

    private String createJsonFileAndReturnPath() {
        return createJsonFileAndReturnPath("test_schema_example.json");
    }

    private String createJsonFileAndReturnPath(String resourcePath) {
        try{
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            InputStream resourceStream = loader.getResourceAsStream(resourcePath);
            String jsonSchemaExample = IOUtils.toString(resourceStream, StandardCharsets.UTF_8);
            File file = File.createTempFile( "janusgraph", "_"+resourcePath);
            file.deleteOnExit();
            FileUtils.writeStringToFile(file, jsonSchemaExample, StandardCharsets.UTF_8);
            return file.getAbsolutePath();
        } catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    private void createData(){
        GraphTraversalSource g = graph.traversal();

        Vertex vertex1 = g.addV("organization").property("time", 12345L)
            .property("name", "test_org1")
            .property("longPropCardinalityList", 123L, "time", 321L)
            .property("longPropCardinalityList", 123L, "time", 213L)
            .property("longPropCardinalityList", 123L, "time", 231L).next();

        Vertex vertex2 = g.addV("organization").property("time", 54321L)
            .property("name", "test_org2")
            .property("longPropCardinalityList", 123L, "time", 321L)
            .property("longPropCardinalityList", 123L, "time", 213L)
            .property("longPropCardinalityList", 123L, "time", 231L).next();

        Vertex vertex3 = g.addV("device").property("time", 1L)
            .property("name", "test_org3")
            .property("longPropCardinalityList", 123L, "time", 321L)
            .property("longPropCardinalityList", 123L, "time", 213L)
            .property("longPropCardinalityList", 123L, "time", 231L).next();

        g.addE("connects").from(vertex1).to(vertex2).property("name", "connectsEdge1").property("time", 123L).next();
        g.addE("connects").from(vertex1).to(vertex3).property("name", "connectsEdge2").property("time", 124L).next();
        g.addE("connects").from(vertex3).to(vertex2).property("name", "connectsEdge3").property("time", 125L).next();
        g.addE("viewed").from(vertex2).to(vertex3).property("name", "connectsEdge4").property("time", 100L).next();

        g.tx().commit();
    }
}
