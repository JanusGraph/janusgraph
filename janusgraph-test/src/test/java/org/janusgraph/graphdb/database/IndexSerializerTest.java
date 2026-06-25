// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.graphdb.database;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransaction;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.BaseTransactionConfigurable;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexFeatures;
import org.janusgraph.diskstorage.indexing.IndexInformation;
import org.janusgraph.diskstorage.indexing.IndexMutation;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.diskstorage.util.DefaultTransaction;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.tinkerpop.optimize.step.Aggregation;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.ParameterIndexField;
import org.janusgraph.graphdb.vertices.StandardVertex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class IndexSerializerTest {

    private static final String REINDEX_TEST_INDEX_BACKEND_NAME = "search";
    private static final String REINDEX_TEST_INDEX_NAME = "nameMixed";
    private static final int REINDEX_TEST_STORAGE_PAGE_SIZE = 5;
    private static final int REINDEX_TEST_BATCH_SIZE = 20;
    private static final int REINDEX_TEST_VERTEX_COUNT = 65;

    @Test
    public void testReindexElementNotAppliesTo() {
        Configuration config = mock(Configuration.class);
        Serializer serializer = mock(Serializer.class);
        EdgeSerializer edgeSerializer = mock(EdgeSerializer.class);
        Map<String, ? extends IndexInformation> indexes = new HashMap<>();

        IndexSerializer mockSerializer = new IndexSerializer(config, edgeSerializer, serializer, indexes, true);
        JanusGraphElement nonIndexableElement = mock(JanusGraphElement.class);
        MixedIndexType mit = mock(MixedIndexType.class);
        doReturn(ElementCategory.VERTEX).when(mit).getElement();

        Map<String, Map<String, List<IndexEntry>>> docStore = new HashMap<>();
        assertFalse("re-index", mockSerializer.reindexElement(nonIndexableElement, mit, docStore));

    }

    @Test
    public void testReindexElementAppliesToWithEntries() {
        Map<String, Map<String, List<IndexEntry>>> docStore = new HashMap<>();
        IndexSerializer mockSerializer = mockSerializer();
        MixedIndexType mit = mock(MixedIndexType.class);

        JanusGraphElement indexableElement = mockIndexAppliesTo(mit, true);

        assertTrue("re-index", mockSerializer.reindexElement(indexableElement, mit, docStore));
        assertEquals("doc store size", 1, docStore.size());

    }

    @Test
    public void testReindexElementAppliesToNoEntries() {
        Map<String, Map<String, List<IndexEntry>>> docStore = new HashMap<>();
        IndexSerializer mockSerializer = mockSerializer();
        MixedIndexType mit = mock(MixedIndexType.class);
        JanusGraphElement indexableElement = mockIndexAppliesTo(mit, false);

        assertFalse("re-index", mockSerializer.reindexElement(indexableElement, mit, docStore));
        assertEquals("doc store size", 0, docStore.size());

    }

    private IndexSerializer mockSerializer() {
        Configuration config = mock(Configuration.class);
        Serializer serializer = mock(Serializer.class);
        EdgeSerializer edgeSerializer = mock(EdgeSerializer.class);
        Map<String, ? extends IndexInformation> indexes = new HashMap<>();
        return spy(new IndexSerializer(config, edgeSerializer, serializer, indexes, true));
    }

    private JanusGraphElement mockIndexAppliesTo(MixedIndexType mit, boolean indexable) {
        String key = "foo";
        String value = "bar";

        JanusGraphElement indexableElement = mockIndexableElement(key, value, indexable);
        ElementCategory ec = ElementCategory.VERTEX;

        doReturn(ec).when(mit).getElement();

        doReturn(false).when(mit).hasSchemaTypeConstraint();

        PropertyKey pk = mock(PropertyKey.class);
        doReturn(1L).when(pk).id();
        doReturn(1L).when(pk).longId();
        doReturn(key).when(pk).name();
        ParameterIndexField pif = mock(ParameterIndexField.class);
        Parameter[] parameter = { new Parameter(key, value) };
        doReturn(parameter).when(pif).getParameters();
        doReturn(SchemaStatus.REGISTERED).when(pif).getStatus();
        doReturn(pk).when(pif).getFieldKey();

        ParameterIndexField[] ifField = { pif };

        doReturn(ifField).when(mit).getFieldKeys();

        return indexableElement;

    }

    private JanusGraphElement mockIndexableElement(String key, String value, boolean indexable) {
        StandardJanusGraphTx tx = mock(StandardJanusGraphTx.class);
        doReturn(tx).when(tx).getNextTx();
        JanusGraphElement indexableElement = spy(new StandardVertex(tx, 1L, ElementLifeCycle.New));
        Property pk2 = new DetachedProperty(key, value);
        Iterator it = Arrays.asList(pk2).iterator();
        doReturn(it).when(indexableElement).properties(key);
        if (indexable)
            doReturn(Arrays.asList(value).iterator()).when(indexableElement).values(key);
        else
            doReturn(new ArrayList<>().iterator()).when(indexableElement).values(key); // skpping the values section!!

        return indexableElement;

    }

    @Test
    public void mixedIndexReindexUsesConfiguredBatchSizeWhenEnabled() throws Exception {
        List<Integer> restoreBatchSizes = runMixedIndexReindex(true);

        assertEquals(REINDEX_TEST_VERTEX_COUNT, restoreBatchSizes.stream().mapToInt(Integer::intValue).sum());
        assertTrue(restoreBatchSizes.stream().allMatch(batchSize -> batchSize <= REINDEX_TEST_BATCH_SIZE));
        assertTrue(restoreBatchSizes.stream().anyMatch(batchSize -> batchSize > REINDEX_TEST_STORAGE_PAGE_SIZE));
    }

    @Test
    public void mixedIndexReindexKeepsStoragePageSizedBatchesWhenDisabled() throws Exception {
        List<Integer> restoreBatchSizes = runMixedIndexReindex(false);

        assertEquals(REINDEX_TEST_VERTEX_COUNT, restoreBatchSizes.stream().mapToInt(Integer::intValue).sum());
        assertTrue(restoreBatchSizes.stream().allMatch(batchSize -> batchSize <= REINDEX_TEST_STORAGE_PAGE_SIZE));
    }

    private List<Integer> runMixedIndexReindex(boolean batchEnabled) throws Exception {
        RecordingIndexProvider.reset();
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        config.set(GraphDatabaseConfiguration.PAGE_SIZE, REINDEX_TEST_STORAGE_PAGE_SIZE);
        config.set(GraphDatabaseConfiguration.INDEX_BACKEND, RecordingIndexProvider.class.getName(), REINDEX_TEST_INDEX_BACKEND_NAME);
        config.set(GraphDatabaseConfiguration.MIXED_INDEX_REINDEX_BATCH_ENABLED, batchEnabled);
        config.set(GraphDatabaseConfiguration.MIXED_INDEX_REINDEX_BATCH_SIZE, REINDEX_TEST_BATCH_SIZE);

        JanusGraph graph = JanusGraphFactory.open(config.getConfiguration());
        try {
            createReindexTestData(graph);
            registerMixedIndex(graph);
            reindex(graph);
            return RecordingIndexProvider.getRestoreBatchSizes();
        } finally {
            graph.close();
        }
    }

    private void createReindexTestData(JanusGraph graph) {
        JanusGraphManagement management = graph.openManagement();
        management.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.commit();

        IntStream.range(0, REINDEX_TEST_VERTEX_COUNT).forEach(vertexNumber -> graph.addVertex("name", "value" + vertexNumber));
        graph.tx().commit();
    }

    private void registerMixedIndex(JanusGraph graph) throws Exception {
        JanusGraphManagement management = graph.openManagement();
        PropertyKey name = management.getPropertyKey("name");
        management.buildIndex(REINDEX_TEST_INDEX_NAME, Vertex.class).addKey(name).buildMixedIndex(REINDEX_TEST_INDEX_BACKEND_NAME);
        management.commit();

        management = graph.openManagement();
        management.updateIndex(management.getGraphIndex(REINDEX_TEST_INDEX_NAME), SchemaAction.REGISTER_INDEX).get();
        management.commit();
        ManagementSystem.awaitGraphIndexStatus(graph, REINDEX_TEST_INDEX_NAME).status(SchemaStatus.REGISTERED).call();
    }

    private void reindex(JanusGraph graph) throws Exception {
        JanusGraphManagement management = graph.openManagement();
        management.updateIndex(management.getGraphIndex(REINDEX_TEST_INDEX_NAME), SchemaAction.REINDEX, 1).get();
        management.commit();
        ManagementSystem.awaitGraphIndexStatus(graph, REINDEX_TEST_INDEX_NAME).status(SchemaStatus.ENABLED).call();
    }

    public static class RecordingIndexProvider implements IndexProvider {

        private static final IndexFeatures FEATURES = new IndexFeatures.Builder()
            .supportedStringMappings(org.janusgraph.core.schema.Mapping.TEXT, org.janusgraph.core.schema.Mapping.STRING)
            .supportsCardinality(Cardinality.SINGLE)
            .supportsCardinality(Cardinality.LIST)
            .supportsCardinality(Cardinality.SET)
            .build();

        private static final List<Integer> RESTORE_BATCH_SIZES = Collections.synchronizedList(new ArrayList<>());

        public RecordingIndexProvider(Configuration config) {
        }

        static void reset() {
            RESTORE_BATCH_SIZES.clear();
        }

        static List<Integer> getRestoreBatchSizes() {
            synchronized (RESTORE_BATCH_SIZES) {
                return new ArrayList<>(RESTORE_BATCH_SIZES);
            }
        }

        @Override
        public void register(String store, String key, KeyInformation information, BaseTransaction tx) {
        }

        @Override
        public void mutate(Map<String, Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever information,
                           BaseTransaction tx) {
        }

        @Override
        public void restore(Map<String, Map<String, List<IndexEntry>>> documents, KeyInformation.IndexRetriever information,
                            BaseTransaction tx) {
            int documentCount = documents.values().stream().mapToInt(Map::size).sum();
            if (documentCount > 0) {
                RESTORE_BATCH_SIZES.add(documentCount);
            }
        }

        @Override
        public Number queryAggregation(IndexQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx,
                                       Aggregation aggregation) {
            return 0;
        }

        @Override
        public Stream<String> query(IndexQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) {
            return Stream.empty();
        }

        @Override
        public Stream<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever information,
                                                     BaseTransaction tx) {
            return Stream.empty();
        }

        @Override
        public Long totals(RawQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) {
            return 0L;
        }

        @Override
        public BaseTransactionConfigurable beginTransaction(BaseTransactionConfig config) throws BackendException {
            return new DefaultTransaction(config);
        }

        @Override
        public void close() {
        }

        @Override
        public void clearStorage() {
            RESTORE_BATCH_SIZES.clear();
        }

        @Override
        public void clearStore(String storeName) {
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public boolean supports(KeyInformation information, JanusGraphPredicate janusgraphPredicate) {
            return true;
        }

        @Override
        public boolean supports(KeyInformation information) {
            return true;
        }

        @Override
        public String mapKey2Field(String key, KeyInformation information) {
            return key;
        }

        @Override
        public IndexFeatures getFeatures() {
            return FEATURES;
        }
    }

}
