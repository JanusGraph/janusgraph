package com.thinkaurelius.titan.graphdb.transaction;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.relations.CacheEdge;
import com.thinkaurelius.titan.graphdb.relations.CacheProperty;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
import com.thinkaurelius.titan.graphdb.types.TypeInspector;
import com.thinkaurelius.titan.graphdb.types.TypeUtil;
import com.tinkerpop.blueprints.Direction;

import javax.annotation.Nullable;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class RelationConstructor {

    public static RelationCache readRelationCache(InternalVertex vertex, Entry data, StandardTitanTx tx) {
        return tx.getEdgeSerializer().readRelation(vertex.getID(), data, false, tx);
    }

    public static Iterable<TitanRelation> readRelation(final InternalVertex vertex, final Iterable<Entry> data, final StandardTitanTx tx) {
        return Iterables.transform(data, new Function<Entry, TitanRelation>() {
            @Override
            public TitanRelation apply(@Nullable Entry entry) {
                return RelationConstructor.readRelation(vertex, entry, tx);
            }
        });
    }

    public static InternalRelation readRelation(final InternalVertex vertex, final Entry data, final StandardTitanTx tx) {
        RelationCache relation = tx.getEdgeSerializer().readRelation(vertex.getID(), data, true, tx);
        return readRelation(vertex,relation,data,tx,tx);
    }

    public static InternalRelation readRelation(final InternalVertex vertex, final Entry data,
                                                final EdgeSerializer serializer, final TypeInspector types,
                                                final VertexFactory vertexFac) {
        RelationCache relation = serializer.readRelation(vertex.getID(), data, true, types);
        return readRelation(vertex,relation,data,types,vertexFac);
    }


    private static InternalRelation readRelation(final InternalVertex vertex, final RelationCache relation,
                                         final Entry data, final TypeInspector types, final VertexFactory vertexFac) {
        InternalRelationType type = TypeUtil.getBaseType((InternalRelationType) types.getExistingType(relation.typeId));

        if (type.isPropertyKey()) {
            assert relation.direction == Direction.OUT;
            return new CacheProperty(relation.relationId, (TitanKey) type, vertex, relation.getValue(), data);
        }

        if (type.isEdgeLabel()) {
            InternalVertex otherVertex = vertexFac.getExistingVertex(relation.getOtherVertexId());
            switch (relation.direction) {
                case IN:
                    return new CacheEdge(relation.relationId, (TitanLabel) type, otherVertex, vertex, (byte) 1, data);

                case OUT:
                    return new CacheEdge(relation.relationId, (TitanLabel) type, vertex, otherVertex, (byte) 0, data);

                default:
                    throw new AssertionError();
            }
        }

        throw new AssertionError();
    }

}
