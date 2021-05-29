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

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.graphdb.database.EdgeSerializer;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.relations.CacheEdge;
import org.janusgraph.graphdb.relations.CacheVertexProperty;
import org.janusgraph.graphdb.relations.RelationCache;
import org.janusgraph.graphdb.types.TypeInspector;
import org.janusgraph.graphdb.types.TypeUtil;

import java.util.Iterator;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class RelationConstructor {

    public static RelationCache readRelationCache(Entry data, StandardJanusGraphTx tx) {
        return tx.getEdgeSerializer().readRelation(data, false, tx);
    }

    public static Iterable<JanusGraphRelation> readRelation(final InternalVertex vertex, final Iterable<Entry> data, final StandardJanusGraphTx tx) {
        return () -> new Iterator<JanusGraphRelation>() {

            private final Iterator<Entry> iterator = data.iterator();
            private JanusGraphRelation current = null;

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public JanusGraphRelation next() {
                current = readRelation(vertex, iterator.next(),tx);
                return current;
            }

            @Override
            public void remove() {
                Preconditions.checkNotNull(current);
                current.remove();
            }
        };
    }

    public static InternalRelation readRelation(final InternalVertex vertex, final Entry data, final StandardJanusGraphTx tx) {
        RelationCache relation = tx.getEdgeSerializer().readRelation(data, true, tx);
        return readRelation(vertex,relation,data,tx,tx);
    }

    public static InternalRelation readRelation(final InternalVertex vertex, final Entry data,
                                                final EdgeSerializer serializer, final TypeInspector types,
                                                final VertexFactory vertexFac) {
        RelationCache relation = serializer.readRelation(data, true, types);
        return readRelation(vertex,relation,data,types,vertexFac);
    }


    private static InternalRelation readRelation(final InternalVertex vertex, final RelationCache relation,
                                         final Entry data, final TypeInspector types, final VertexFactory vertexFac) {
        InternalRelationType type = TypeUtil.getBaseType((InternalRelationType) types.getExistingRelationType(relation.typeId));

        if (type.isPropertyKey()) {
            assert relation.direction == Direction.OUT;
            return new CacheVertexProperty(relation.relationId, (PropertyKey) type, vertex, relation.getValue(), data);
        }

        if (type.isEdgeLabel()) {
            InternalVertex otherVertex = vertexFac.getInternalVertex(relation.getOtherVertexId());
            switch (relation.direction) {
                case IN:
                    return new CacheEdge(relation.relationId, (EdgeLabel) type, otherVertex, vertex, data);

                case OUT:
                    return new CacheEdge(relation.relationId, (EdgeLabel) type, vertex, otherVertex, data);

                default:
                    throw new AssertionError();
            }
        }

        throw new AssertionError();
    }

}
