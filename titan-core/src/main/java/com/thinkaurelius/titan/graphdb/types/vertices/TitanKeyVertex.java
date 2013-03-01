package com.thinkaurelius.titan.graphdb.types.vertices;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.PropertyKeyDefinition;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.tinkerpop.blueprints.Element;

public class TitanKeyVertex extends TitanTypeVertex implements TitanKey {

    private PropertyKeyDefinition definition = null;

    public TitanKeyVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    @Override
    public PropertyKeyDefinition getDefinition() {
        if (definition == null) {
            synchronized (this) {
                if (definition==null) {
                    definition = QueryUtil.queryHiddenUniqueProperty(this, SystemKey.PropertyKeyDefinition)
                            .getValue(PropertyKeyDefinition.class);
                    Preconditions.checkNotNull(definition);
                }
            }
        }
        return definition;
    }

    @Override
    public Class<?> getDataType() {
        return getDefinition().getDataType();
    }


    @Override
    public Iterable<String> getIndexes(Class<? extends Element> elementType) {
        return getDefinition().getIndexes(elementType);
    }

    @Override
    public boolean hasIndex(String name, Class<? extends Element> elementType) {
        return getDefinition().hasIndex(name,elementType);
    }

    @Override
    public final boolean isPropertyKey() {
        return true;
    }

    @Override
    public final boolean isEdgeLabel() {
        return false;
    }

}
