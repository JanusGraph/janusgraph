package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.base.Preconditions;

/**
 * //TODO: Merge into {@link com.thinkaurelius.titan.graphdb.types.TypeAttributeType} and find better name for the result!
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum TypeRelatedCategory {

    RELATION_INDEX("relationIndex");



    private final String name;
    private final Class<?> modifierDataType;

    private TypeRelatedCategory(String name) {
        this(name,null);
    }

    private TypeRelatedCategory(String name, Class<?> modifierDataType) {
        Preconditions.checkNotNull(name);
        this.name=name;
        this.modifierDataType=modifierDataType;
    }

    public boolean hasModifier() {
        return modifierDataType!=null;
    }

    public<T> Class<T> getModifierDataType() {
        Preconditions.checkState(hasModifier(),"TypeRelatedCategory [%s] does not have a modifier",this);
        return (Class<T>)modifierDataType;
    }

    @Override
    public String toString() {
        return name;
    }
}
