package com.thinkaurelius.titan.graphdb.relations;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.util.ExceptionFactory;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public final class RelationIdentifier {

    public static final String TOSTRING_DELIMITER = ":";

    private final long outVertexId;
    private final long typeId;
    private final long relationId;

    private RelationIdentifier(final long outVertexId, final long typeId, final long relationId) {
        this.outVertexId=outVertexId;
        this.typeId=typeId;
        this.relationId=relationId;
    }
    
    static final RelationIdentifier get(TitanProperty property) {
        if (property.hasID()) {
            return new RelationIdentifier(property.getVertex().getID(),
                                            property.getPropertyKey().getID(),
                                            property.getID());
        } else return null;
    }
    
    static final RelationIdentifier get(TitanEdge edge) {
        if (edge.hasID()) {
            return new RelationIdentifier(edge.getVertex(Direction.OUT).getID(),
                                            edge.getTitanLabel().getID(),
                                            edge.getID());
        } else return null;
    }
    
    @Override
    public int hashCode() {
        return Long.valueOf(relationId).hashCode()+
                Long.valueOf(outVertexId).hashCode()*743 +
                Long.valueOf(typeId).hashCode()*3011;
    }
    
    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (!getClass().isInstance(other)) return false;
        RelationIdentifier oth = (RelationIdentifier)other;
        return relationId==oth.relationId && outVertexId==oth.outVertexId && typeId==oth.typeId;
    }
    
    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append(relationId).append(TOSTRING_DELIMITER).append(outVertexId)
                .append(TOSTRING_DELIMITER).append(typeId);
        return s.toString();
    }
    
    public TitanEdge findEdge(TitanTransaction tx) {
        TitanVertex v = tx.getVertex(outVertexId);
        if (v==null) return null;
        TitanVertex v2 = tx.getVertex(typeId);
        if (v2==null) return null;
        if (!(v2 instanceof TitanLabel)) throw new IllegalArgumentException("Invalid RelationIdentifier: typeID does not reference a label");
        TitanLabel label = (TitanLabel)v2;
        for (TitanEdge e : v.getTitanEdges(Direction.OUT,label)) {
            if (e.getID()==relationId) return e;
        }
        return null;
    }
    
    public static final RelationIdentifier parse(String id) {
        String[] elements = id.split(TOSTRING_DELIMITER);
        if (elements.length!=3) return null;
        try {
            return new RelationIdentifier(Long.parseLong(elements[1]),
                                            Long.parseLong(elements[2]),
                                            Long.parseLong(elements[0]));
        } catch (NumberFormatException e) {
            //throw new IllegalArgumentException("Invalid id - each token expected to be a number",e);
            return null;
        }
    }

    public static final RelationIdentifier get(long[] ids) {
        if (ids.length!=3) return null;
        for (int i=0;i<3;i++) {
            //Preconditions.checkArgument(ids[i]>=0,"Non-negative numbers expected");
            if (ids[i]<0) return null;
        }
        return new RelationIdentifier(ids[0],ids[1],ids[2]);
    }

    public static final RelationIdentifier get(int[] ids) {
        if (ids.length!=3) return null;
        for (int i=0;i<3;i++) {
            //Preconditions.checkArgument(ids[i]>=0,"Non-negative numbers expected");
            if (ids[i]<0) return null;
        }
        return new RelationIdentifier(ids[0],ids[1],ids[2]);
    }
    
}
