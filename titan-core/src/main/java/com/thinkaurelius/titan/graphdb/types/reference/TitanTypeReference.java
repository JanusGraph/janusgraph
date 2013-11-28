package com.thinkaurelius.titan.graphdb.types.reference;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.types.TypeAttribute;
import com.thinkaurelius.titan.graphdb.types.TypeAttributeType;
import com.thinkaurelius.titan.graphdb.types.system.EmptyVertex;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanTypeVertex;
import com.tinkerpop.blueprints.Direction;
import org.apache.commons.lang.StringUtils;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class TitanTypeReference extends EmptyVertex implements InternalType {

    private final long id;
    private final String name;
    private final TypeAttribute.Map definition;

    protected TitanTypeReference(TitanTypeVertex type) {
        this(type.getID(),type.getName(),type.getDefinition());
    }

    protected TitanTypeReference(long id, String name, TypeAttribute.Map definition) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        Preconditions.checkNotNull(definition);
        Preconditions.checkArgument(id>0);
        this.id=id;
        this.name = name;
        this.definition = definition;
    }

    @Override
    public String getName() {
        return name;
    }

    public TypeAttribute.Map getDefinition() {
        return definition;
    }

    @Override
    public long getID() {
        return id;
    }

    //####### IDENTICAL TO TitanTypeVertex

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
