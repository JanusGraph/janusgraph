package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class SimpleTitanQuery extends SimpleAtomicQuery implements TitanQuery {


    public SimpleTitanQuery(InternalTitanVertex n) {
        super(n);
    }

    public SimpleTitanQuery(InternalTitanTransaction tx, long nodeid) {
        super(tx,nodeid);
    }

    SimpleTitanQuery(SimpleTitanQuery q) {
        super(q);
    }

    @Override
    public SimpleTitanQuery clone() {
        SimpleTitanQuery q = new SimpleTitanQuery(this);
        return q;
    }

    @Override
    public TitanQuery types(TitanType... type) {
        Preconditions.checkNotNull(type,"Label(s) or key(s) expected");
        if (type.length==0) return this;

        for (int i=0;i<type.length;i++) {
            Preconditions.checkNotNull(type[i],"Label or key cannot be null at position " + i);
        }
        if (type.length==1) {
            super.type(type[0]);
            return this;
        }
        else {
            List<TitanQuery> disjunction = new ArrayList<TitanQuery>(type.length);
            for (int i=0;i<type.length;i++) {
                AtomicQuery q2 = this.clone();
                q2.type(type[i]);
                disjunction.add(q2);
            }
            return new ComplexTitanQuery(disjunction);
        }
    }

    @Override
    public TitanQuery labels(String... type) {
        Preconditions.checkNotNull(type);
        TitanType[] types = new TitanType[type.length];
        int size = 0;
        for (int i=0;i<type.length;i++) {
            TitanType t = getType(type[i]);
            if (t!=null) {
                types[size]=t;
                size++;
            }
        }
        if (size<type.length) {
            if (size==0) return EmptyAtomicQuery.INSTANCE;
            types = Arrays.copyOf(types,size);
        }
        return types(types);
    }

    @Override
    public TitanQuery keys(String... type) {
        return labels(type);
    }



}

