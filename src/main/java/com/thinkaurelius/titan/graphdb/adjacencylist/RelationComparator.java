package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;
import com.thinkaurelius.titan.graphdb.types.InternalTitanType;
import com.thinkaurelius.titan.graphdb.types.TypeDefinition;

import java.util.Comparator;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class RelationComparator implements Comparator<InternalRelation> {
    
    private final int position;
    
    private RelationComparator(int position) {
        Preconditions.checkArgument(position==0 || position==1);
        this.position=position;
    }
    
    
    @Override
    public int compare(InternalRelation r1, InternalRelation r2) {
        if (r1.equals(r2)) return 0;
        TitanType t1 = r1.getType(), t2 = r2.getType();
        if (!t1.equals(t2)) return TypeComparator.INSTANCE.compare(t1, t2);
        else {
            assert t1.equals(t2);
            TypeDefinition tdef = ((InternalTitanType)t1).getDefinition();
            tdef.getKeySignature()
        }
    }
}
