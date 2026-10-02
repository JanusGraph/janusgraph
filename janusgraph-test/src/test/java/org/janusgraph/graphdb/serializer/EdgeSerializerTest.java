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

package org.janusgraph.graphdb.serializer;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.StorageSetup;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.graphdb.database.EdgeSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.relations.RelationCache;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.system.ImplicitKey;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class EdgeSerializerTest {

    @Test
    public void testValueOrdering() {
        StandardJanusGraph graph = (StandardJanusGraph) StorageSetup.getInMemoryGraph();
        try {
            JanusGraphManagement management = graph.openManagement();
            management.makeEdgeLabel("father").multiplicity(Multiplicity.MANY2ONE).make();
            for (int i=1;i<=5;i++) management.makePropertyKey("key" + i).dataType(Integer.class).make();
            management.commit();

            JanusGraphVertex v1 = graph.addVertex(), v2 = graph.addVertex();
            JanusGraphEdge e1 = v1.addEdge("father",v2);
            for (int i=1;i<=5;i++) e1.property("key"+i,i);

            graph.tx().commit();

            e1.remove();
            graph.tx().commit();
        } finally {
            graph.close();
        }
    }

    private static final int WEIGHT = 5;
    private static final int SINCE = 7;

    //What the checks saw: how many entries, how many of them with properties, and how many complete at the header
    private static final class Seen {
        int entries;
        int withProperties;
        int completeAtTheHeader;
    }

    //A relation without properties used to be parsed again on every read of its properties, because its empty
    //property map was taken for a header-only parse. A full parse is kept whether or not it found properties, and a
    //header-only parse which finds nothing after the header is complete. The layouts covered: an edge label of each
    //multiplicity read at both ends, since the other end and the relation id sit in the column or in the value
    //depending on the multiplicity and the direction; a property key of each cardinality; an edge label and a property
    //key with a signature, which is written after the header even without a value; and the entries of edge indexes
    //with an ascending and a descending sort key and of a vertex property index, which carry their sort key
    @Test
    public void testRelationIsFullyParsedOnce() {
        StandardJanusGraph graph = (StandardJanusGraph) StorageSetup.getInMemoryGraph();
        try {
            JanusGraphManagement management = graph.openManagement();
            PropertyKey weight = management.makePropertyKey("weight").dataType(Integer.class).make();
            PropertyKey since = management.makePropertyKey("since").dataType(Integer.class).make();
            List<String> labels = new ArrayList<>();
            for (Multiplicity multiplicity : Multiplicity.values()) {
                management.makeEdgeLabel("edge" + multiplicity).multiplicity(multiplicity).make();
                labels.add("edge" + multiplicity);
            }
            management.makeEdgeLabel("signed").signature(weight).make();
            labels.add("signed");
            for (Order order : new Order[]{Order.asc, Order.desc}) {
                EdgeLabel indexed = management.makeEdgeLabel("indexed" + order).make();
                management.buildEdgeIndex(indexed, "weight" + order, Direction.BOTH, order, weight);
                labels.add("indexed" + order);
            }
            List<String> keys = new ArrayList<>();
            for (Cardinality cardinality : Cardinality.values()) {
                PropertyKey key = management.makePropertyKey("property" + cardinality).dataType(String.class)
                    .cardinality(cardinality).make();
                if (cardinality == Cardinality.LIST) {
                    management.buildPropertyIndex(key, "since" + cardinality, Order.desc, since);
                }
                keys.add("property" + cardinality);
            }
            management.makePropertyKey("signedProperty").dataType(String.class).signature(since).make();
            keys.add("signedProperty");
            management.commit();

            //Every relation on vertices of its own, so that no multiplicity constraint gets in the way
            List<Object> vertexIds = new ArrayList<>();
            for (String label : labels) {
                for (boolean withProperty : new boolean[]{false, true}) {
                    JanusGraphVertex out = graph.addVertex();
                    JanusGraphEdge edge = out.addEdge(label, graph.addVertex());
                    if (withProperty) edge.property("weight", WEIGHT);
                    vertexIds.add(out.id());
                }
            }
            for (String key : keys) {
                for (boolean withProperty : new boolean[]{false, true}) {
                    JanusGraphVertex vertex = graph.addVertex();
                    JanusGraphVertexProperty<String> property = vertex.property(key, "value");
                    if (withProperty) property.property("since", SINCE);
                    vertexIds.add(vertex.id());
                }
            }
            graph.tx().commit();

            StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.newTransaction();
            try {
                EdgeSerializer serializer = tx.getEdgeSerializer();
                PropertyKey weightKey = tx.getPropertyKey("weight");
                PropertyKey sinceKey = tx.getPropertyKey("since");
                Seen seen = new Seen();
                for (Object id : vertexIds) {
                    InternalVertex vertex = (InternalVertex) tx.getVertex(id);
                    List<InternalRelation> relations = new ArrayList<>();
                    vertex.query().direction(Direction.OUT).edges().forEach(edge -> relations.add((InternalRelation) edge));
                    vertex.query().properties().forEach(property -> relations.add((InternalRelation) property));
                    for (InternalRelation relation : relations) {
                        InternalRelationType type = (InternalRelationType) relation.getType();
                        PropertyKey key = relation.isEdge() ? weightKey : sinceKey;
                        int value = relation.isEdge() ? WEIGHT : SINCE;
                        boolean hasProperties = relation.getValueDirect(key) != null;
                        //An edge is stored at both of its ends, a vertex property at its vertex
                        for (int position = 0; position < (relation.isEdge() ? 2 : 1); position++) {
                            checkParsedOnce(serializer, tx, relation, type, position, hasProperties,
                                !hasProperties && type.getSignature().length == 0, key, value, seen);
                            for (InternalRelationType index : type.getRelationIndexes()) {
                                if (index != type) {
                                    checkParsedOnce(serializer, tx, relation, index, position, hasProperties, false,
                                        key, value, seen);
                                }
                            }
                        }
                    }
                }
                //Edges of 8 labels at both ends with the entries of their 2 edge indexes, and vertex properties of 4
                //keys with the entries of their 1 index, each with and without a property
                assertEquals(8 * 2 * 2 + 2 * 2 * 2 + 4 * 2 + 2, seen.entries);
                assertEquals(seen.entries / 2, seen.withProperties);
                //The entries without a property of the 7 edge labels, at both ends, and the 3 property keys which
                //have no signature
                assertEquals(7 * 2 + 3, seen.completeAtTheHeader);
            } finally {
                tx.rollback();
            }
        } finally {
            graph.close();
        }
    }

    private static void checkParsedOnce(EdgeSerializer serializer, StandardJanusGraphTx tx, InternalRelation relation,
                                        InternalRelationType type, int position, boolean hasProperties,
                                        boolean completeAtTheHeader, PropertyKey key, int value, Seen seen) {
        final String what = relation + " written as " + type + " at position " + position;

        //A full parse serves every later read, full or header-only, whether or not it found properties
        final Entry readFully = serializer.writeRelation(relation, type, position, tx);
        final RelationCache full = serializer.readRelation(readFully, false, tx);
        assertTrue(full.isFullyParsed(), what);
        assertEquals(hasProperties, full.hasProperties(), what);
        assertSame(full, serializer.readRelation(readFully, false, tx), what);
        assertSame(full, serializer.readRelation(readFully, true, tx), what);
        if (hasProperties) {
            final Integer found = full.get(key.longId());
            assertEquals(value, found, what);
        } else {
            assertNull(full.get(key.longId()), what);
        }

        //A header-only parse which finds nothing after the header is complete at once; any other is completed on the
        //first full read, and only once
        final Entry readHeaderFirst = serializer.writeRelation(relation, type, position, tx);
        final RelationCache header = serializer.readRelation(readHeaderFirst, true, tx);
        assertSame(header, serializer.readRelation(readHeaderFirst, true, tx), what);
        assertEquals(completeAtTheHeader, header.isFullyParsed(), what);
        final RelationCache completed = serializer.readRelation(readHeaderFirst, false, tx);
        if (completeAtTheHeader) {
            assertSame(header, completed, what);
        } else {
            assertNotSame(header, completed, what);
        }
        assertTrue(completed.isFullyParsed(), what);
        assertEquals(hasProperties, completed.hasProperties(), what);
        assertSame(completed, serializer.readRelation(readHeaderFirst, false, tx), what);
        assertEquals(full.relationId, completed.relationId, what);
        assertEquals(full.typeId, completed.typeId, what);
        assertEquals(full.direction, completed.direction, what);
        assertEquals(full.getValue(), completed.getValue(), what);

        seen.entries++;
        if (full.hasProperties()) seen.withProperties++;
        if (header.isFullyParsed()) seen.completeAtTheHeader++;

        if (completeAtTheHeader) {
            //Metadata which a full parse turns into a property, such as a timestamp, keeps a parse header-only
            final StaticArrayEntry timestamped = serializer.writeRelation(relation, type, position, tx);
            timestamped.setMetaData(EntryMetaData.TIMESTAMP, 42L);
            assertFalse(serializer.readRelation(timestamped, true, tx).isFullyParsed(), what);
            final RelationCache withTimestamp = serializer.readRelation(timestamped, false, tx);
            assertTrue(withTimestamp.hasProperties(), what);
            final Long timestamp = withTimestamp.get(ImplicitKey.TIMESTAMP.longId());
            assertEquals(Long.valueOf(42L), timestamp, what);

            //Other metadata, such as the row key of a grouped multi-key read, doesn't
            final StaticArrayEntry grouped = serializer.writeRelation(relation, type, position, tx);
            grouped.setMetaData(EntryMetaData.ROW_KEY, BufferUtil.getLongBuffer(1));
            final RelationCache rowKeyed = serializer.readRelation(grouped, true, tx);
            assertTrue(rowKeyed.isFullyParsed(), what);
            assertFalse(rowKeyed.hasProperties(), what);
            assertSame(rowKeyed, serializer.readRelation(grouped, false, tx), what);

            //Nor does a row key hide a timestamp beside it
            final StaticArrayEntry groupedAndTimestamped = serializer.writeRelation(relation, type, position, tx);
            groupedAndTimestamped.setMetaData(EntryMetaData.ROW_KEY, BufferUtil.getLongBuffer(1));
            groupedAndTimestamped.setMetaData(EntryMetaData.TIMESTAMP, 42L);
            assertFalse(serializer.readRelation(groupedAndTimestamped, true, tx).isFullyParsed(), what);
            assertTrue(serializer.readRelation(groupedAndTimestamped, false, tx).hasProperties(), what);
        }
    }

}
