package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;
import com.thinkaurelius.titan.graphdb.types.InternalTitanType;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.tinkerpop.blueprints.Direction;

import java.util.Comparator;
import java.util.Iterator;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class RelationComparator implements Comparator<InternalRelation> {

    public static final RelationComparator OUT = new RelationComparator(1);
    public static final RelationComparator IN = new RelationComparator(0);

    private final int position;
    
    private RelationComparator(int position) {
        Preconditions.checkArgument(position==0 || position==1);
        this.position=position;
    }
    
    @Override
    public int compare(InternalRelation r1, InternalRelation r2) {
        Preconditions.checkNotNull(r1);
        Preconditions.checkNotNull(r2);
        if (r1 instanceof DummyInternalRelation || r2 instanceof DummyInternalRelation) {
            if (r1 instanceof DummyInternalRelation) return ((DummyInternalRelation)r1).compareTo(r2);
            else return -((DummyInternalRelation)r2).compareTo(r1);
        } else return properCompare(r1,r2);
    }

    public int properCompare(InternalRelation r1, InternalRelation r2) {
        if (r1==r2 ||
                (r1.hasID() && r2.hasID() && r1.getID()==r2.getID())) {
            assert r1.equals(r2);
            return 0;
        }
        TitanType t1 = r1.getType(), t2 = r2.getType();
        int typecompare = TypeComparator.INSTANCE.compare(t1, t2);
        if (typecompare!=0) return typecompare;
        else {
            // 1) Compare primary key values
            assert t1.equals(t2);
            for (String key : ((InternalTitanType)t1).getDefinition().getKeySignature()) {
                int keycompare = compareOnKey(r1, r2, key);
                if (keycompare!=0) return keycompare;
            }
            if (position==1 && t1.isFunctional()) {
                assert r1.equals(r2);
                return 0;
            }
            // 2) Compare property objects or other vertices
            if (r1.isProperty()) {
                Preconditions.checkArgument(r2.isProperty() && position==1);
                Object o1 = ((TitanProperty)r1).getAttribute();
                Object o2 = ((TitanProperty)r2).getAttribute();
                Preconditions.checkArgument(o1!=null && o2!=null);
                if (!o1.equals(o2)) {
                    int objectcompare = 0;
                    if (Comparable.class.isAssignableFrom(((TitanKey)t1).getDataType())) {
                        objectcompare = ((Comparable)o1).compareTo(o2);
                    } else {
                        objectcompare = System.identityHashCode(o1)-System.identityHashCode(o2);
                    }
                    if (objectcompare!=0) return objectcompare;
                }
            } else {
                Preconditions.checkArgument(r1.isEdge() && r2.isEdge());
                int vertexcompare = compareVertices(r1.getVertex(position), r2.getVertex(position));
                if (vertexcompare!=0) return vertexcompare;
            }
            //TODO: if graph is simple, return 0
            // 3)compare relation ids
            if (!r1.hasID() || !r2.hasID()) {
                if (r1.hasID()) return -1;
                else if (r2.hasID()) return 1;
                else {
                    assert r1.equals(r2);
                    return 0;
                }
            } else return Long.valueOf(r1.getID()).compareTo(r2.getID());
        }
    }
    
    public static int compareVertices(TitanVertex v1, TitanVertex v2) {
        if (!v1.hasID() || !v2.hasID()) {
            if (v1.hasID()) return -1;
            else if (v2.hasID()) return 1;
            else {
                int result = System.identityHashCode(v1)-System.identityHashCode(v2);
                Preconditions.checkArgument(v1==v2 || result!=0,"Invalid hash code function");
                return result;
            }
        } else {
            return Long.valueOf(v1.getID()).compareTo(v2.getID());
        }
    }
    
    public static int compareOnKey(InternalRelation r1, InternalRelation r2, String keyType) {
        TitanType type = r1.getTransaction().getType(keyType);
        Preconditions.checkArgument(type.isFunctional());
        if (type.isPropertyKey()) {
            TitanKey key = (TitanKey)type;
            Object o1 = r1.getProperty(key);
            Object o2 = r2.getProperty(key);
            if (o1==null || o2==null) {
                if (o1!=null) return -1;
                else if (o2!=null) return 1;
                else return 0;
            } else {
                Preconditions.checkArgument(Comparable.class.isAssignableFrom(key.getDataType()));
                return ((Comparable)o1).compareTo(o2);
            }
        } else {
            TitanLabel label = (TitanLabel)type;
            Preconditions.checkArgument(label.isUnidirected());
            TitanEdge e1 = Iterables.getOnlyElement(r1.getTitanEdges(Direction.OUT,label),null);
            TitanEdge e2 = Iterables.getOnlyElement(r2.getTitanEdges(Direction.OUT,label),null);
            if (e1==null || e2==null) {
                if (e1!=null) return -1;
                else if (e2!=null) return 1;
                else return 0;
            } else {
                return compareVertices(e1.getVertex(Direction.IN),e2.getVertex(Direction.IN));
            }
        }

    }
}
