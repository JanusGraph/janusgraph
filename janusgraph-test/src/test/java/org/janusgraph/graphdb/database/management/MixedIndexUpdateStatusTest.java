// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.graphdb.database.management;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.IndexSerializerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

//A mixed index keeps a SchemaStatus per field rather than one status on the index itself, so updateIndex selects the
//fields whose status the action applies to. When that selection is empty the action used to reach setStatusVertex,
//whose precondition is guaranteed to fail for a mixed index and carries no message, so the caller got a bare
//IllegalArgumentException. Re-running an idempotent schema script is enough to reach it.
public class MixedIndexUpdateStatusTest {

    private static final String INDEX_BACKEND_NAME = "search";
    private static final String INDEX_NAME = "nameMixed";
    private static final String PROPERTY_NAME = "name";

    private JanusGraph graph;

    @BeforeEach
    public void openGraph() {
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        config.set(GraphDatabaseConfiguration.INDEX_BACKEND,
            IndexSerializerTest.RecordingIndexProvider.class.getName(), INDEX_BACKEND_NAME);
        graph = JanusGraphFactory.open(config.getConfiguration());
    }

    @AfterEach
    public void closeGraph() {
        if (graph != null) {
            graph.close();
        }
    }

    //A key created in the same management transaction as the index cannot hold data yet, so no reindex is needed and
    //the index is enabled straight away
    private void createMixedIndexOnNewKey() {
        final JanusGraphManagement management = graph.openManagement();
        final PropertyKey name = management.makePropertyKey(PROPERTY_NAME).dataType(String.class)
            .cardinality(Cardinality.SINGLE).make();
        management.buildIndex(INDEX_NAME, Vertex.class).addKey(name).buildMixedIndex(INDEX_BACKEND_NAME);
        management.commit();
    }

    //An index over a key which already exists starts as INSTALLED, because the existing data still has to be indexed
    private void createMixedIndexOnExistingKey() {
        JanusGraphManagement management = graph.openManagement();
        management.makePropertyKey(PROPERTY_NAME).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.commit();

        management = graph.openManagement();
        management.buildIndex(INDEX_NAME, Vertex.class).addKey(management.getPropertyKey(PROPERTY_NAME))
            .buildMixedIndex(INDEX_BACKEND_NAME);
        management.commit();
    }

    private void updateIndex(SchemaAction action) {
        final JanusGraphManagement management = graph.openManagement();
        try {
            management.updateIndex(management.getGraphIndex(INDEX_NAME), action);
            management.commit();
        } catch (RuntimeException e) {
            management.rollback();
            throw e;
        }
    }

    private void awaitStatus(SchemaStatus status) throws Exception {
        ManagementSystem.awaitGraphIndexStatus(graph, INDEX_NAME).status(status).call();
    }

    @Test
    public void shouldRejectAnActionWhichMatchesNoFieldWithADescriptiveError() throws Exception {
        createMixedIndexOnNewKey();
        awaitStatus(SchemaStatus.ENABLED);

        //REGISTER_INDEX applies to INSTALLED and DISABLED, so an already enabled index matches no field. This is the
        //idempotent schema script case from the issue
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> updateIndex(SchemaAction.REGISTER_INDEX));

        //Previously the action reached setStatusVertex, whose Preconditions.checkArgument carries no message at all,
        //so the caller got a bare IllegalArgumentException with nothing to act on
        final String message = e.getMessage();
        assertNotNull(message, "the rejection must say why it was rejected");
        assertTrue(message.contains("REGISTER_INDEX"), message);
        assertTrue(message.contains(INDEX_NAME), message);
        //The status which blocked the action, and the statuses which would have allowed it
        assertTrue(message.contains(SchemaStatus.ENABLED.name()), message);
        assertTrue(message.contains(SchemaStatus.INSTALLED.name()), message);
    }

    @Test
    public void shouldStillRegisterAnInstalledIndex() throws Exception {
        //The fields are INSTALLED, which REGISTER_INDEX applies to, so the normal path is unaffected
        createMixedIndexOnExistingKey();
        updateIndex(SchemaAction.REGISTER_INDEX);
        awaitStatus(SchemaStatus.REGISTERED);
    }

    @Test
    public void shouldStillDisableAnEnabledIndex() throws Exception {
        //DISABLE_INDEX applies to ENABLED, so it matches the field and is carried out
        createMixedIndexOnNewKey();
        awaitStatus(SchemaStatus.ENABLED);
        updateIndex(SchemaAction.DISABLE_INDEX);
        awaitStatus(SchemaStatus.DISABLED);
    }
}
