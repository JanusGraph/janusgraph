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
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.IndexSerializerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
        ClearStoreRecordingIndexProvider.reset();
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        config.set(GraphDatabaseConfiguration.INDEX_BACKEND,
            ClearStoreRecordingIndexProvider.class.getName(), INDEX_BACKEND_NAME);
        graph = JanusGraphFactory.open(config.getConfiguration());
    }

    //DISCARD_INDEX clears the index backend before it changes the status, so whether the backend was touched is the
    //only way to tell a rejected discard from one which got half way
    public static class ClearStoreRecordingIndexProvider extends IndexSerializerTest.RecordingIndexProvider {

        private static final AtomicInteger CLEAR_STORE_CALLS = new AtomicInteger();

        public ClearStoreRecordingIndexProvider(Configuration config) {
            super(config);
        }

        static void reset() {
            CLEAR_STORE_CALLS.set(0);
        }

        static int clearStoreCalls() {
            return CLEAR_STORE_CALLS.get();
        }

        @Override
        public void clearStore(String storeName) {
            CLEAR_STORE_CALLS.incrementAndGet();
            super.clearStore(storeName);
        }
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
        //Every status which would have allowed the action, so that the reader knows what to reach for
        assertTrue(message.contains(SchemaStatus.INSTALLED.name()), message);
        assertTrue(message.contains(SchemaStatus.DISABLED.name()), message);
        //The status which blocked it, named against the field which holds it. Asserting the pair rather than the
        //status alone keeps ENABLED appearing anywhere else in the message from satisfying this
        assertTrue(message.contains(PROPERTY_NAME + "=" + SchemaStatus.ENABLED.name()), message);
    }

    @Test
    public void shouldStillRegisterAnInstalledIndex() throws Exception {
        //The fields are INSTALLED, which REGISTER_INDEX applies to, so the normal path is unaffected
        createMixedIndexOnExistingKey();
        updateIndex(SchemaAction.REGISTER_INDEX);
        awaitStatus(SchemaStatus.REGISTERED);
    }

    @Test
    public void shouldRejectDiscardWhichMatchesNoFieldWithoutClearingTheIndexBackend() throws Exception {
        //DISCARD_INDEX applies to DISABLED, REGISTERED, DISCARDED and WRITE_ONLY_ENABLED, so an enabled index matches
        //no field. Before the guard the action cleared the index backend first and only then failed in
        //setStatusVertex, so the documents were already gone while the status change had not happened
        createMixedIndexOnNewKey();
        awaitStatus(SchemaStatus.ENABLED);

        assertThrows(IllegalArgumentException.class, () -> updateIndex(SchemaAction.DISCARD_INDEX));
        assertEquals(0, ClearStoreRecordingIndexProvider.clearStoreCalls(),
            "a rejected discard must not have touched the index backend");
    }

    @Test
    public void shouldRejectDropOfAnIndexWhichIsNotDiscarded() throws Exception {
        //DROP_INDEX applies only to DISCARDED. Before the guard schemaVertex.remove() ran regardless, dropping the
        //schema vertex and orphaning every document still in the index backend
        createMixedIndexOnNewKey();
        awaitStatus(SchemaStatus.ENABLED);

        assertThrows(IllegalArgumentException.class, () -> updateIndex(SchemaAction.DROP_INDEX));

        final JanusGraphManagement management = graph.openManagement();
        assertNotNull(management.getGraphIndex(INDEX_NAME), "the index must survive a rejected drop");
        management.rollback();
    }

    @Test
    public void shouldStillDiscardAndDropADisabledIndex() throws Exception {
        //The sequence which must keep working, and the one every existing test in the tree uses: disable, discard,
        //drop. Each action matches a field, so none of them is rejected
        createMixedIndexOnNewKey();
        awaitStatus(SchemaStatus.ENABLED);
        updateIndex(SchemaAction.DISABLE_INDEX);
        awaitStatus(SchemaStatus.DISABLED);

        updateIndex(SchemaAction.DISCARD_INDEX);
        awaitStatus(SchemaStatus.DISCARDED);
        assertEquals(1, ClearStoreRecordingIndexProvider.clearStoreCalls(),
            "an accepted discard must have cleared the index backend");

        updateIndex(SchemaAction.DROP_INDEX);

        final JanusGraphManagement management = graph.openManagement();
        assertNull(management.getGraphIndex(INDEX_NAME));
        management.rollback();
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
