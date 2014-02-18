package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;

import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public enum TypeDefinitionCategory {

    UNIQUENESS(boolean[].class),
    UNIQUENESS_LOCK(boolean[].class),
    HIDDEN(Boolean.class),
    MODIFIABLE(Boolean.class),
    SORT_KEY(long[].class),
    SIGNATURE(long[].class),
    INDEXES(IndexType[].class),
    DATATYPE(Class.class),
    UNIDIRECTIONAL(Boolean.class),
    SORT_ORDER(Order.class),
    INDEX_PARAMETERS(IndexParameters[].class),

    RELATION_INDEX();


    private final RelationCategory relationCategory;
    private final Class dataType;

    private TypeDefinitionCategory() {
        this(RelationCategory.EDGE,null);
    }

    private TypeDefinitionCategory(Class<?> dataType) {
        this(RelationCategory.PROPERTY, dataType);
    }

    private TypeDefinitionCategory(RelationCategory relCat, Class<?> dataType) {
        Preconditions.checkArgument(relCat!=null && relCat.isProper());
        Preconditions.checkArgument(relCat==RelationCategory.EDGE || dataType !=null);
        this.relationCategory = relCat;
        this.dataType = dataType;
    }

    public boolean hasDataType() {
        return dataType !=null;
    }

    public Class<?> getDataType() {
        Preconditions.checkState(hasDataType());
        return dataType;
    }

    public boolean isProperty() {
        return relationCategory==RelationCategory.PROPERTY;
    }

    public boolean isEdge() {
        return relationCategory==RelationCategory.EDGE;
    }

    public boolean verifyAttribute(Object attribute) {
        Preconditions.checkState(dataType !=null);
        return attribute != null && dataType.equals(attribute.getClass());
    }

    public Object defaultValue(TypeDefinitionMap map) {
        switch(this) {
            case SORT_ORDER: return Order.ASC;
            default: return null;
        }
    }

    static final Set<TypeDefinitionCategory> PROPERTY_KEY_DEFINITION_CATEGORIES = ImmutableSet.of(UNIQUENESS, UNIQUENESS_LOCK,
            HIDDEN, MODIFIABLE, SORT_KEY, SORT_ORDER, SIGNATURE, INDEXES, INDEX_PARAMETERS, DATATYPE);

    static final Set<TypeDefinitionCategory> EDGE_LABEL_DEFINITION_CATEGORIES = ImmutableSet.of(UNIQUENESS, UNIQUENESS_LOCK,
            HIDDEN, MODIFIABLE, SORT_KEY, SORT_ORDER, SIGNATURE, UNIDIRECTIONAL);

}
