// Copyright 2020 JanusGraph Authors
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

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.log.kcvs.KCVSLog;
import org.janusgraph.diskstorage.mixed.utils.MixedIndexUtilsConfigOptions;
import org.janusgraph.diskstorage.mixed.utils.processor.DynamicErrorDistanceCircleProcessor;
import org.janusgraph.diskstorage.mixed.utils.processor.FixedErrorDistanceCircleProcessor;
import org.janusgraph.diskstorage.mixed.utils.processor.NoTransformCircleProcessor;
import org.janusgraph.graphdb.JanusGraphIndexTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;

import java.time.Duration;
import java.util.stream.IntStream;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.FORCE_INDEX_USAGE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_READ_INTERVAL;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_SEND_DELAY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.MANAGEMENT_LOG;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class ElasticsearchJanusGraphIndexTest extends JanusGraphIndexTest {

    @Container
    protected static JanusGraphElasticsearchContainer esr = new JanusGraphElasticsearchContainer();

    public ElasticsearchJanusGraphIndexTest() {
        super(true, true, true);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        String[] indexBackends = getIndexBackends();
        ModifiableConfiguration config =  esr.setConfiguration(getStorageConfiguration(), indexBackends);
        for (String indexBackend : indexBackends) {
            config.set(GraphDatabaseConfiguration.INDEX_MAX_RESULT_SET_SIZE, 3, indexBackend);
        }
        return config.getConfiguration();
    }

    public abstract ModifiableConfiguration getStorageConfiguration();

    @Test
    public void indexShouldExistAfterCreation() throws Exception {
        PropertyKey key = mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.buildIndex("verticesByName", Vertex.class).addKey(key).buildMixedIndex("search");
        mgmt.commit();

        String expectedIndexName = INDEX_NAME.getDefaultValue() + "_" + "verticesByName".toLowerCase();
        assertTrue(esr.indexExists(expectedIndexName));
    }

    @Test
    public void indexShouldNotExistAfterDeletion() throws Exception {
        clopen(option(LOG_SEND_DELAY, MANAGEMENT_LOG), Duration.ZERO,
            option(KCVSLog.LOG_READ_LAG_TIME, MANAGEMENT_LOG), Duration.ofMillis(50),
            option(LOG_READ_INTERVAL, MANAGEMENT_LOG), Duration.ofMillis(250),
            option(FORCE_INDEX_USAGE), true
        );

        String indexName = "mixed";
        String propertyName = "prop";

        makeKey(propertyName, String.class);
        finishSchema();

        //Never create new indexes while a transaction is active
        graph.getOpenTransactions().forEach(JanusGraphTransaction::rollback);
        mgmt = graph.openManagement();

        registerIndex(indexName, Vertex.class, propertyName);
        enableIndex(indexName);
        disableIndex(indexName);
        discardIndex(indexName);
        dropIndex(indexName);

        String expectedIndexName = INDEX_NAME.getName() + "_" + indexName.toLowerCase();
        assertFalse(esr.indexExists(expectedIndexName));
    }

    @Test
    public void writingAnItemLargerThanPermittedChunkLimitFails() {
        PropertyKey key = mgmt.makePropertyKey("some-field").dataType(Integer.class).make();
        mgmt.buildIndex("bulkTooLargeWriteTestIndex", Vertex.class).addKey(key).buildMixedIndex(INDEX);
        mgmt.buildIndex("equalityLookupIndex", Vertex.class).addKey(key).buildCompositeIndex();
        mgmt.commit();

        //Confirm we're able to successfully write initially
        Vertex initiallyWrittenVertex = graph.traversal().addV().property(key.name(), 1).next();
        graph.tx().commit();

        //Retrieve the vertex again based on the composite index and then mixed index that, confirming it's in both
        Vertex initialVertexEqualityLookup = graph.traversal().V().has(key.name(), P.eq(1)).next();
        Vertex initialVertexRangeLookup = graph.traversal().V().has(key.name(), P.gt(0)).next();

        Assertions.assertEquals(initiallyWrittenVertex.id(), initialVertexEqualityLookup.id(),
            "Should have returned the same vertex");
        Assertions.assertEquals(initiallyWrittenVertex.id(), initialVertexRangeLookup.id(),
            "Should have returned the same vertex");

        //Now write a second vertex, but with a limit that prevents the mixed index write from succeeding
        //Writes to mixed indices are "best effort", so a "successful" write that failed to write to a mixed index
        //is still a success. However, lookups via the mixed index's predicates will now be blind to the vertex
        clopen(option(ElasticSearchIndex.BULK_CHUNK_SIZE_LIMIT_BYTES, INDEX), 1);
        Vertex secondWriteAttemptVertex = graph.traversal().addV().property(key.name(), 2).next();
        graph.tx().commit();
        Vertex secondVertexEqualityLookup = graph.traversal().V().has(key.name(), P.eq(2)).next();
        boolean secondVertexRangeLookup = graph.traversal().V().has(key.name(), P.gt(1)).hasNext();

        Assertions.assertEquals(secondWriteAttemptVertex.id(), secondVertexEqualityLookup.id(),
            "Should have returned the same vertex");
        Assertions.assertFalse(secondVertexRangeLookup, "The lookup for the second vertex using the mixed index " +
            "predicate should have failed to find it due to a silent mutation failure to the mixed index due to the " +
            "chunk size limit");
    }

    @Test
    //Disabled to not slow down CI/CD builds given there's nothing to assert
    //intended for manual observation of the bulk chunking to ElasticSearch
    @Disabled
    public void manuallyObserveBulkWritingChunking() {
        clopen(option(ElasticSearchIndex.BULK_CHUNK_SIZE_LIMIT_BYTES, INDEX), 100);
        PropertyKey key = mgmt.makePropertyKey("some-field").dataType(String.class).make();
        mgmt.buildIndex("testChunkingIndex", Vertex.class).addKey(key).buildMixedIndex(INDEX);
        mgmt.commit();

        //Write 10 vertices that will each individually be split up into their own chunks due to the limit configured
        IntStream.range(0, 10).forEach(i -> graph.traversal().addV().property(key.name(), "foobar").toList());
        graph.tx().commit();
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }

    @Override
    public boolean supportsWildcardQuery() {
        return true;
    }

    @Override
    protected boolean supportsCollections() {
        return true;
    }

    @Override
    public boolean supportsGeoPointExistsQuery() {
        return true;
    }

    @Override
    public boolean supportsGeoShapePrefixTreeMapping() {
        return esr.getEsMajorVersion().getValue() <= 7;
    }

    @Test
    public void testCircleBKDIndexMappingCanBeUsedWithDefaultProcessor(){
        clopen();
        createIndexedBKDGeoshape();
        Assertions.assertTrue(addVertexWithCircleValueProperty());
    }

    @Test
    public void testCircleBKDIndexMappingCanBeUsedWithNoChangeProcessor(){
        clopen(option(MixedIndexUtilsConfigOptions.BKD_CIRCLE_PROCESSOR_CLASS, INDEX), NoTransformCircleProcessor.SHORTHAND);
        createIndexedBKDGeoshape();
        boolean vertexAddedAndFound = addVertexWithCircleValueProperty();
        if(esr.getVersion().startsWith("6.0")){
            // ElasticSearch before 6.6 used Prefix Tree
            Assertions.assertTrue(vertexAddedAndFound);
        } else {
            // Error while committing index mutations. Thus, the circle isn't added (as expected).
            Assertions.assertFalse(vertexAddedAndFound);
        }
    }

    @Test
    public void testCircleBKDIndexMappingCanBeUsedWithFixedProcessor(){
        clopen(option(MixedIndexUtilsConfigOptions.BKD_CIRCLE_PROCESSOR_CLASS, INDEX), FixedErrorDistanceCircleProcessor.SHORTHAND);
        createIndexedBKDGeoshape();
        Assertions.assertTrue(addVertexWithCircleValueProperty());
    }

    @Test
    public void testCircleBKDIndexMappingCanBeUsedWithDynamicProcessor(){
        clopen(option(MixedIndexUtilsConfigOptions.BKD_CIRCLE_PROCESSOR_CLASS, INDEX), DynamicErrorDistanceCircleProcessor.SHORTHAND);
        createIndexedBKDGeoshape();
        Assertions.assertTrue(addVertexWithCircleValueProperty());
    }

    @Test
    public void testCircleBKDIndexMappingCanBeUsedWithCustomProcessor(){
        MutableBoolean preProcessingNormalCircle = new MutableBoolean(false);
        MutableBoolean processedIntoPolygon = new MutableBoolean(false);
        TestCircleProcessor.setPreProcessConsumer(geoshape -> {
            if(Geoshape.Type.CIRCLE.equals(geoshape.getType())){
                preProcessingNormalCircle.setTrue();
            } else {
                Assertions.fail("Circle had to be used");
            }
        });
        TestCircleProcessor.setPostProcessConsumer(geoshape -> {
            if(Geoshape.Type.POLYGON.equals(geoshape.getType())){
                processedIntoPolygon.setTrue();
            } else {
                Assertions.fail("Expected Polygon Geoshape after process but got "+geoshape.getType().toString());
            }
        });

        clopen(option(MixedIndexUtilsConfigOptions.BKD_CIRCLE_PROCESSOR_CLASS, INDEX), TestCircleProcessor.class.getName());

        createIndexedBKDGeoshape();
        Assertions.assertTrue(addVertexWithCircleValueProperty());
        Assertions.assertTrue(preProcessingNormalCircle.booleanValue());
        Assertions.assertTrue(processedIntoPolygon.booleanValue());
    }

    private void createIndexedBKDGeoshape(){
        String indexName = "mixed";
        PropertyKey geoshapeProp = mgmt.makePropertyKey("foo").dataType(Geoshape.class).cardinality(Cardinality.SINGLE).make();
        mgmt.buildIndex(indexName, Vertex.class).addKey(geoshapeProp, new Parameter<>("mapping", Mapping.BKD)).buildMixedIndex(INDEX);
        finishSchema();
    }

    private boolean addVertexWithCircleValueProperty(){
        Geoshape circle = Geoshape.circle(50, 50, 50);
        Vertex addedVertex = graph.traversal().addV().property("foo", circle).next();
        graph.tx().commit();
        return graph.traversal().V().has("foo",
            Geo.geoWithin(Geoshape.box(-90, -180, 90, 180)))
            .toList().stream().anyMatch(vertex -> addedVertex.id().equals(vertex.id()) && circle.equals(vertex.value("foo")));
    }

}
