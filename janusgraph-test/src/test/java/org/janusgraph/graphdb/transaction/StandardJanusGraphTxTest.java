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
package org.janusgraph.graphdb.transaction;

import org.easymock.EasyMockSupport;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.schema.DefaultSchemaMaker;
import org.janusgraph.core.schema.PropertyKeyMaker;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.EdgeSerializer;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.query.index.IndexSelectionStrategy;
import org.janusgraph.graphdb.query.index.ThresholdBasedIndexSelectionStrategy;
import org.junit.jupiter.api.Test;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.notNull;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class StandardJanusGraphTxTest extends EasyMockSupport {

    @Test
    public void testGetOrCreatePropertyKey() {
        StandardJanusGraphTx tx = createTxWithMockedInternals();
        tx.getOrCreatePropertyKey("Foo", "Bar");
        Exception e = null;
        try {
            tx.getOrCreatePropertyKey("Baz", "Quuz");
        } catch (IllegalArgumentException ex) {
            e = ex;
        }
        tx.getOrCreatePropertyKey("Qux", "Quux");
        assertNotNull(e, "getOrCreatePropertyKey should throw an Exception when the relationType is not a PropertyKey");
        verifyAll();
    }


    private StandardJanusGraphTx createTxWithMockedInternals() {
        StandardJanusGraph mockGraph = createMock(StandardJanusGraph.class);
        TransactionConfiguration txConfig = createMock(TransactionConfiguration.class);
        GraphDatabaseConfiguration gdbConfig = createMock(GraphDatabaseConfiguration.class);
        TimestampProvider tsProvider = createMock(TimestampProvider.class);
        Serializer mockSerializer = createMock(Serializer.class);
        EdgeSerializer mockEdgeSerializer = createMock(EdgeSerializer.class);
        IndexSerializer mockIndexSerializer = createMock(IndexSerializer.class);
        RelationType relationType = createMock(RelationType.class);
        IDManager idManager = createMock(IDManager.class);
        PropertyKey propertyKey = createMock(PropertyKey.class);
        DefaultSchemaMaker defaultSchemaMaker = createMock(DefaultSchemaMaker.class);
        IndexSelectionStrategy indexSelectionStrategy = createMock(ThresholdBasedIndexSelectionStrategy.class);

        expect(mockGraph.getConfiguration()).andReturn(gdbConfig);
        expect(mockGraph.isOpen()).andReturn(true).anyTimes();
        expect(mockGraph.getDataSerializer()).andReturn(mockSerializer);
        expect(mockGraph.getEdgeSerializer()).andReturn(mockEdgeSerializer);
        expect(mockGraph.getIndexSerializer()).andReturn(mockIndexSerializer);
        expect(mockGraph.getIDManager()).andReturn(idManager);
        expect(mockGraph.getIndexSelector()).andReturn(indexSelectionStrategy);

        expect(gdbConfig.getTimestampProvider()).andReturn(tsProvider);

        expect(txConfig.isSingleThreaded()).andReturn(true);
        expect(txConfig.hasPreloadedData()).andReturn(false);
        expect(txConfig.hasVerifyExternalVertexExistence()).andReturn(false);
        expect(txConfig.hasVerifyInternalVertexExistence()).andReturn(false);
        expect(txConfig.getVertexCacheSize()).andReturn(6);
        expect(txConfig.isReadOnly()).andReturn(true);
        expect(txConfig.getDirtyVertexSize()).andReturn(2);
        expect(txConfig.getIndexCacheWeight()).andReturn(2L);
        expect(txConfig.getGroupName()).andReturn(null);
        expect(txConfig.getAutoSchemaMaker()).andReturn(defaultSchemaMaker);

        expect(defaultSchemaMaker.makePropertyKey(isA(PropertyKeyMaker.class), notNull())).andReturn(propertyKey);

        expect(relationType.isPropertyKey()).andReturn(false);

        expect(propertyKey.isPropertyKey()).andReturn(true);

        replayAll();

        StandardJanusGraphTx partialMock = createMockBuilder(StandardJanusGraphTx.class)
           .withConstructor(mockGraph, txConfig)
           .addMockedMethod("getRelationType")
           .createMock();

        expect(partialMock.getRelationType("Foo")).andReturn(null);
        expect(partialMock.getRelationType("Qux")).andReturn(propertyKey);
        expect(partialMock.getRelationType("Baz")).andReturn(relationType);

        replay(partialMock);
        return partialMock;

    }
}
