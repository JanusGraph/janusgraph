package com.thinkaurelius.titan.graphdb.types.vertices;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.EdgeLabelDefinition;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;

public class TitanLabelVertex extends TitanTypeVertex implements TitanLabel {

    private EdgeLabelDefinition definition = null;

    public TitanLabelVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    @Override
    public EdgeLabelDefinition getDefinition() {
        if (definition == null) {
            synchronized (this) {
                if (definition==null) {
                    definition = QueryUtil.queryHiddenUniqueProperty(this, SystemKey.EdgeTypeDefinition)
                            .getValue(EdgeLabelDefinition.class);
                    Preconditions.checkNotNull(definition);
                }
            }
        }
        return definition;
    }

    public boolean isDirected() {
        return !isUnidirected();
    }

    public boolean isUnidirected() {
        return getDefinition().isUnidirectional();
    }

    @Override
    public final boolean isPropertyKey() {
        return false;
    }

    @Override
    public final boolean isEdgeLabel() {
        return true;
    }

}
