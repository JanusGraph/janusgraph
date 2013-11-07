package com.thinkaurelius.titan.graphdb.relations;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.tinkerpop.blueprints.Direction;

import java.util.Comparator;


/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class RelationComparator implements Comparator<InternalRelation> {

    private final InternalVertex vertex;

    public RelationComparator(InternalVertex v) {
        Preconditions.checkNotNull(v);
        this.vertex = v;
    }

    @Override
    public int compare(final InternalRelation r1, final InternalRelation r2) {
        if (r1.equals(r2)) return 0;

        //0) RelationType
        int reltypecompare = (r1.isProperty()?1:2) - (r2.isProperty()?1:2);
        if (reltypecompare != 0) return reltypecompare;

        //1) TitanType
        InternalType t1 = (InternalType) r1.getType(), t2 = (InternalType) r2.getType();
        int typecompare = t1.compareTo(t2);
        if (typecompare != 0) return typecompare;
        assert t1.equals(t2);

        //2) Direction
        Direction dir1 = null, dir2 = null;
        for (int i = 0; i < r1.getLen(); i++)
            if (r1.getVertex(i).equals(vertex)) {
                dir1 = EdgeDirection.fromPosition(i);
                break;
            }
        for (int i = 0; i < r2.getLen(); i++)
            if (r2.getVertex(i).equals(vertex)) {
                dir2 = EdgeDirection.fromPosition(i);
                break;
            }
        assert dir1 != null && dir2 != null; // ("Either relation is not incident on vertex [%s]", vertex);
        int dirCompare = EdgeDirection.position(dir1) - EdgeDirection.position(dir2);
        if (dirCompare != 0) return dirCompare;

        // Breakout: If type&direction are the same and the type is unique in the direction it follows that the relations are the same
        if (t1.isUnique(dir1)) return 0;

        // 3) Compare sort key values
        for (long typeid : t1.getSortKey()) {
            int keycompare = compareOnKey(r1, r2, typeid, t1.getSortOrder());
            if (keycompare != 0) return keycompare;
        }
        // 4) Compare property objects or other vertices
        if (r1.isProperty()) {
            Object o1 = ((TitanProperty) r1).getValue();
            Object o2 = ((TitanProperty) r2).getValue();
            Preconditions.checkArgument(o1 != null && o2 != null);
            if (!o1.equals(o2)) {
                int objectcompare = 0;
                if (Comparable.class.isAssignableFrom(((TitanKey) t1).getDataType())) {
                    objectcompare = ((Comparable) o1).compareTo(o2);
                } else {
                    objectcompare = System.identityHashCode(o1) - System.identityHashCode(o2);
                }
                if (objectcompare != 0) return objectcompare;
            }
        } else {
            Preconditions.checkArgument(r1.isEdge() && r2.isEdge());
            int vertexcompare = r1.getVertex(EdgeDirection.position(dir1.opposite())).
                    compareTo(r2.getVertex(EdgeDirection.position(dir1.opposite())));
            if (vertexcompare != 0) return vertexcompare;
        }
        //TODO: if graph is simple, return 0
        // 5)compare relation ids
        return r1.compareTo(r2);
    }

    public static int compareValues(Object v1, Object v2, Order order) {
        return compareValues(v1,v2)*(order==Order.DESC?-1:1);
    }

    public static int compareValues(Object v1, Object v2) {
        if (v1 == null || v2 == null) {
            if (v1 != null) return -1;
            else if (v2 != null) return 1;
            else return 0;
        } else {
            Preconditions.checkArgument(v1 instanceof Comparable && v2 instanceof Comparable, "Encountered invalid values");
            return ((Comparable) v1).compareTo(v2);
        }
    }

    public int compareOnKey(TitanRelation r1, TitanRelation r2, long typeid, Order order) {
        TitanType type = vertex.tx().getExistingType(typeid);
        Object v1, v2;
        if (type.isPropertyKey()) {
            TitanKey key = (TitanKey) type;
            v1 = r1.getProperty(key);
            v2 = r2.getProperty(key);
        } else {
            TitanLabel label = (TitanLabel) type;
            v1 = r1.getProperty(label);
            v2 = r2.getProperty(label);
        }
        return compareValues(v1, v2,order);
    }
}