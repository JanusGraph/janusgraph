package com.thinkaurelius.titan.graphdb.types.vertices;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.transaction.RelationConstructor;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionDescription;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionMap;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.vertices.CacheVertex;
import com.tinkerpop.blueprints.Direction;

public abstract class TitanTypeVertex extends CacheVertex implements InternalType {

    private String name = null;
    private TypeDefinitionMap definition = null;

    public TitanTypeVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    @Override
    public String getName() {
        if (name == null) {
            TitanProperty p;
            if (isLoaded()) {
                StandardTitanTx tx = tx();
                p = (TitanProperty) Iterables.getOnlyElement(RelationConstructor.readRelation(this,
                                            tx.getGraph().getSchemaCache().getTypeRelations(getID(), SystemKey.TypeName, Direction.OUT, tx()),
                                            tx), null);
            } else {
                p = Iterables.getOnlyElement(query().
                        includeHidden().type(SystemKey.TypeName).properties(), null);
            }
            Preconditions.checkState(p!=null,"Could not find type for id: %s",getID());
            name = p.getValue(String.class);
        }
        assert name != null;
        return name;
    }

    public TypeDefinitionMap getDefinition() {
        if (definition == null) {
            TypeDefinitionMap def = new TypeDefinitionMap();
            Iterable<TitanProperty> ps;
            if (isLoaded()) {
                StandardTitanTx tx = tx();
                ps = (Iterable)RelationConstructor.readRelation(this,
                        tx.getGraph().getSchemaCache().getTypeRelations(getID(), SystemKey.TypeDefinitionProperty, Direction.OUT, tx()),
                        tx);
            } else {
                ps = query().includeHidden().type(SystemKey.TypeDefinitionProperty).properties();
            }
            for (TitanProperty property : ps) {
                Preconditions.checkArgument(property.getPropertyKey().equals(SystemKey.TypeDefinitionProperty));
                TypeDefinitionDescription desc = property.getProperty(SystemKey.TypeDefinitionDesc);
                Preconditions.checkArgument(desc!=null && desc.getCategory().isProperty());
                def.setValue(desc.getCategory(), property.getValue());
            }
            definition = def;
        }
        return definition;
    }

    //####### IDENTICAL TO TitanTypeReference

    @Override
    public String toString() {
        return getName();
    }

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
