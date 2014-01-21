package com.thinkaurelius.titan.graphdb.transaction;

import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.relations.CacheEdge;
import com.thinkaurelius.titan.graphdb.relations.CacheProperty;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
import com.tinkerpop.blueprints.Direction;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class RelationConstructor {

    public static RelationCache readRelation(InternalVertex vertex, Entry data, StandardTitanTx tx) {
        return tx.getEdgeSerializer().readRelation(vertex.getID(), data, false, tx);
    }

    public static InternalRelation readRelation(final InternalVertex vertex, final Entry data, final EdgeSerializer edgeSerializer) {
        StandardTitanTx tx = vertex.tx();
        RelationCache relation = edgeSerializer.readRelation(vertex.getID(), data, true, tx);
        return readRelation(vertex,relation,data,tx);
    }

    private static InternalRelation readRelation(final InternalVertex vertex, final RelationCache relation,
                                         final Entry data, final StandardTitanTx tx) {
        TitanType type = tx.getExistingType(relation.typeId);

        if (type.isPropertyKey()) {
            assert relation.direction == Direction.OUT;
            return new CacheProperty(relation.relationId, (TitanKey) type, vertex, relation.getValue(), data);
        }

        if (type.isEdgeLabel()) {
            InternalVertex otherVertex = tx.getExistingVertex(relation.getOtherVertexId());
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
