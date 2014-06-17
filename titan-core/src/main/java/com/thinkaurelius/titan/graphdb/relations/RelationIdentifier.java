package com.thinkaurelius.titan.graphdb.relations;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.system.ImplicitKey;
import com.thinkaurelius.titan.util.encoding.LongEncoding;
import com.tinkerpop.blueprints.Direction;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Arrays;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public final class RelationIdentifier {

    public static final String TOSTRING_DELIMITER = "-";

    private final long outVertexId;
    private final long typeId;
    private final long relationId;
    private final long inVertexId;

    private RelationIdentifier(final long outVertexId, final long typeId, final long relationId, final long inVertexId) {
        this.outVertexId = outVertexId;
        this.typeId = typeId;
        this.relationId = relationId;
        this.inVertexId = inVertexId;
    }

    static final RelationIdentifier get(InternalRelation r) {
        if (r.hasId()) {
            return new RelationIdentifier(r.getVertex(0).getID(),
                    r.getType().getID(),
                    r.getID(),(r.isEdge()?r.getVertex(1).getID():0));
        } else return null;
    }

    public long getRelationId() {
        return relationId;
    }

    public static final RelationIdentifier get(long[] ids) {
        if (ids.length != 3 && ids.length!=4) throw new IllegalArgumentException("Not a valid relation identifier: " + Arrays.toString(ids));
        for (int i = 0; i < 3; i++) {
            if (ids[i] < 0)  throw new IllegalArgumentException("Not a valid relation identifier: " + Arrays.toString(ids));
        }
        return new RelationIdentifier(ids[1], ids[2], ids[0], ids.length==4?ids[3]:0);
    }

    public static final RelationIdentifier get(int[] ids) {
        if (ids.length != 3 && ids.length!=4) throw new IllegalArgumentException("Not a valid relation identifier: " + Arrays.toString(ids));
        for (int i = 0; i < 3; i++) {
            if (ids[i] < 0)  throw new IllegalArgumentException("Not a valid relation identifier: " + Arrays.toString(ids));
        }
        return new RelationIdentifier(ids[1], ids[2], ids[0], ids.length==4?ids[3]:0);
    }

    public long[] getLongRepresentation() {
        long[] r = new long[3+(inVertexId!=0?1:0)];
        r[0]=relationId;
        r[1]=outVertexId;
        r[2]=typeId;
        if (inVertexId!=0) r[3]=inVertexId;
        return r;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(outVertexId).append(typeId).append(relationId).append(inVertexId).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        else if (!getClass().isInstance(other)) return false;
        RelationIdentifier oth = (RelationIdentifier) other;
        return relationId == oth.relationId && outVertexId == oth.outVertexId && typeId == oth.typeId && inVertexId==oth.inVertexId;
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append(LongEncoding.encode(relationId)).append(TOSTRING_DELIMITER).append(LongEncoding.encode(outVertexId))
                .append(TOSTRING_DELIMITER).append(LongEncoding.encode(typeId));
        if (inVertexId!=0) s.append(TOSTRING_DELIMITER).append(LongEncoding.encode(inVertexId));
        return s.toString();
    }

    public static final RelationIdentifier parse(String id) {
        String[] elements = id.split(TOSTRING_DELIMITER);
        if (elements.length != 3 && elements.length != 4) throw new IllegalArgumentException("Not a valid relation identifier: " + id);
        try {
            return new RelationIdentifier(LongEncoding.decode(elements[1]),
                    LongEncoding.decode(elements[2]),
                    LongEncoding.decode(elements[0]),
                    elements.length==4?LongEncoding.decode(elements[3]):0);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid id - each token expected to be a number",e);
        }
    }

    TitanRelation findRelation(TitanTransaction tx) {
        TitanVertex v = tx.getVertex(outVertexId);
        if (v == null) return null;
        TitanVertex typeVertex = tx.getVertex(typeId);
        if (typeVertex == null) return null;
        if (!(typeVertex instanceof RelationType))
            throw new IllegalArgumentException("Invalid RelationIdentifier: typeID does not reference a type");

        RelationType type = (RelationType)typeVertex;
        Iterable<? extends TitanRelation> rels;
        if (((RelationType) typeVertex).isEdgeLabel()) {
            Direction dir = Direction.OUT;
            TitanVertex other = tx.getVertex(inVertexId);
            if (other==null) return null;
            if (((StandardTitanTx)tx).isPartitionedVertex(v) && !((StandardTitanTx)tx).isPartitionedVertex(other)) { //Swap for likely better performance
                TitanVertex tmp = other;
                other = v;
                v = tmp;
                dir = Direction.IN;
            }
            rels = ((VertexCentricQueryBuilder)v.query()).noPartitionRestriction().types((EdgeLabel) type).direction(dir).adjacent(other).titanEdges();
        } else {
            rels = ((VertexCentricQueryBuilder)v.query()).noPartitionRestriction().types((PropertyKey)type).properties();
        }

        for (TitanRelation r : rels) {
            //Find current or previous relation
            if (r.getID() == relationId ||
                    ((r instanceof StandardRelation) && ((StandardRelation)r).getPreviousID()==relationId)) return r;
        }
        return null;
    }

    public TitanEdge findEdge(TitanTransaction tx) {
        TitanRelation r = findRelation(tx);
        if (r==null) return null;
        else if (r instanceof TitanEdge) return (TitanEdge)r;
        else throw new UnsupportedOperationException("Referenced relation is a property not an edge");
    }

    public TitanProperty findProperty(TitanTransaction tx) {
        TitanRelation r = findRelation(tx);
        if (r==null) return null;
        else if (r instanceof TitanProperty) return (TitanProperty)r;
        else throw new UnsupportedOperationException("Referenced relation is a edge not a property");
    }





}
