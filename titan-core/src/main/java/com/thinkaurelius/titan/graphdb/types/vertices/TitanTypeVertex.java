package com.thinkaurelius.titan.graphdb.types.vertices;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.TypeAttribute;
import com.thinkaurelius.titan.graphdb.types.TypeAttributeType;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.vertices.CacheVertex;
import com.tinkerpop.blueprints.Direction;

public abstract class TitanTypeVertex extends CacheVertex implements InternalType {

    private String name = null;
    private TypeAttribute.Map definition = null;

    public TitanTypeVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    @Override
    public String getName() {
        if (name == null) {
            TitanProperty p = Iterables.getOnlyElement(query().
                    includeHidden().type(SystemKey.TypeName).properties(), null);
            Preconditions.checkState(p!=null,"Could not find type for id: %s",getID());
            name = p.getValue(String.class);
        }
        assert name != null;
        return name;
    }

    protected TypeAttribute.Map getDefinition() {
        if (definition == null) {
            TypeAttribute.Map def = new TypeAttribute.Map();
            for (TitanProperty p : query().includeHidden().
                    type(SystemKey.TypeDefinition).properties()) {
                def.add(p.getValue(TypeAttribute.class));
            }
            definition = def;
        }
        return definition;
    }


    @Override
    public String toString() {
        return getName();
    }

    @Override
    public boolean isUnique(Direction direction) {
        return getDefinition().getValue(TypeAttributeType.UNIQUENESS, boolean[].class)[EdgeDirection.position(direction)];
    }

    @Override
    public boolean uniqueLock(Direction direction) {
        return isUnique(direction) && getDefinition().getValue(TypeAttributeType.UNIQUENESS_LOCK, boolean[].class)[EdgeDirection.position(direction)];
    }

    @Override
    public boolean isStatic(Direction direction) {
        return getDefinition().getValue(TypeAttributeType.STATIC, boolean[].class)[EdgeDirection.position(direction)];
    }

    @Override
    public long[] getSortKey() {
        return getDefinition().getValue(TypeAttributeType.SORT_KEY, long[].class);
    }

    @Override
    public Order getSortOrder() {
        return getDefinition().getValue(TypeAttributeType.SORT_ORDER, Order.class);
    }

    @Override
    public long[] getSignature() {
        return getDefinition().getValue(TypeAttributeType.SIGNATURE, long[].class);
    }

    @Override
    public boolean isModifiable() {
        return getDefinition().getValue(TypeAttributeType.MODIFIABLE, boolean.class);
    }

    @Override
    public boolean isHidden() {
        return getDefinition().getValue(TypeAttributeType.HIDDEN, boolean.class);
    }

}
