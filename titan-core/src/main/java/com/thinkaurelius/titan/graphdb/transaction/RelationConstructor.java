package com.thinkaurelius.titan.graphdb.transaction;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.relations.CacheEdge;
import com.thinkaurelius.titan.graphdb.relations.CacheProperty;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
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
        return readRelation(vertex,relation,data,tx);
    }

    private static InternalRelation readRelation(final InternalVertex vertex, final RelationCache relation,
                                         final Entry data, final StandardTitanTx tx) {
        InternalRelationType type = (InternalRelationType) tx.getExistingType(relation.typeId);

        InternalRelationType base = type.getBaseType();
        boolean invertDirection = false;
        if (base!=null) {
            invertDirection = type.invertedBaseDirection();
            type = base;
        }

        if (type.isPropertyKey()) {
            assert relation.direction == Direction.OUT;
            assert !invertDirection;
            return new CacheProperty(relation.relationId, (TitanKey) type, vertex, relation.getValue(), data);
        }

        if (type.isEdgeLabel()) {
            InternalVertex otherVertex = tx.getExistingVertex(relation.getOtherVertexId());
            Direction dir = relation.direction;
            if (invertDirection) dir=dir.opposite();
            switch (dir) {
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
