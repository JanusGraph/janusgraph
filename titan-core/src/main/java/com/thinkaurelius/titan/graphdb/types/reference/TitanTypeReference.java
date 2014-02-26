package com.thinkaurelius.titan.graphdb.types.reference;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionMap;
import com.thinkaurelius.titan.graphdb.types.system.EmptyVertex;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanTypeVertex;
import com.tinkerpop.blueprints.Direction;
import org.apache.commons.lang.StringUtils;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class TitanTypeReference extends EmptyVertex implements InternalRelationType {

    private final long id;
    private final String name;
    private final TypeDefinitionMap definition;

    protected TitanTypeReference(TitanTypeVertex type) {
        this(type.getID(),type.getName(),type.getDefinition());
    }

    protected TitanTypeReference(long id, String name, TypeDefinitionMap definition) {
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

    public TypeDefinitionMap getDefinition() {
        return definition;
    }

    @Override
    public long getID() {
        return id;
    }

    //####### IDENTICAL TO TitanTypeVertex

    @Override
    public boolean isUnique(Direction direction) {
        return getDefinition().getValue(TypeDefinitionCategory.UNIQUENESS, boolean[].class)[EdgeDirection.position(direction)];
    }

    @Override
    public boolean uniqueLock(Direction direction) {
        return isUnique(direction) && getDefinition().getValue(TypeDefinitionCategory.UNIQUENESS_LOCK, boolean[].class)[EdgeDirection.position(direction)];
    }

    @Override
    public long[] getSortKey() {
        return getDefinition().getValue(TypeDefinitionCategory.SORT_KEY, long[].class);
    }

    @Override
    public Order getSortOrder() {
        return getDefinition().getValue(TypeDefinitionCategory.SORT_ORDER, Order.class);
    }

    @Override
    public long[] getSignature() {
        return getDefinition().getValue(TypeDefinitionCategory.SIGNATURE, long[].class);
    }

    @Override
    public boolean isModifiable() {
        return getDefinition().getValue(TypeDefinitionCategory.MODIFIABLE, boolean.class);
    }

    @Override
    public boolean isHidden() {
        return getDefinition().getValue(TypeDefinitionCategory.HIDDEN, boolean.class);
    }


}
