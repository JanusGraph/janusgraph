package com.thinkaurelius.titan.graphdb.types.vertices;

import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.graphdb.adjacencylist.AdjacencyListFactory;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.vertices.CacheVertex;
import com.tinkerpop.blueprints.Direction;

public abstract class TitanTypeVertex extends CacheVertex implements InternalType {

    private String name = null;

    public TitanTypeVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    @Override
    public String getName() {
        if (name == null) {
            name = getDefinition().getName();
        }
        return name;
    }

    @Override
    public boolean isUnique(Direction direction) {
        return getDefinition().isUnique(direction);
    }

    @Override
    public boolean uniqueLock(Direction direction) {
        return getDefinition().uniqueLock(direction);
    }

    @Override
    public TypeGroup getGroup() {
        return getDefinition().getGroup();
    }

    @Override
    public boolean isHidden() {
        return getDefinition().isHidden();
    }

    @Override
    public boolean isStatic(Direction dir) {
        return getDefinition().isStatic(dir);
    }

    @Override
    public boolean isModifiable() {
        return getDefinition().isModifiable();
    }


}
