package com.thinkaurelius.titan.graphdb.types;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.Order;

import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public enum TypeAttributeType {

    UNIQUENESS(boolean[].class),
    UNIQUENESS_LOCK(boolean[].class),
    STATIC(boolean[].class),
    HIDDEN(Boolean.class),
    MODIFIABLE(Boolean.class),
    SORT_KEY(long[].class),
    SIGNATURE(long[].class),
    INDEXES(IndexType[].class),
    DATATYPE(Class.class),
    UNIDIRECTIONAL(Boolean.class),
    SORT_ORDER(Order.class),
    INDEX_PARAMETERS(IndexParameters[].class);

    static final Set<TypeAttributeType> PROPERTY_KEY_TYPES = ImmutableSet.of(UNIQUENESS, UNIQUENESS_LOCK, STATIC,
            HIDDEN, MODIFIABLE, SORT_KEY, SORT_ORDER, SIGNATURE, INDEXES, INDEX_PARAMETERS, DATATYPE);

    static final Set<TypeAttributeType> EDGE_LABEL_TYPES = ImmutableSet.of(UNIQUENESS, UNIQUENESS_LOCK, STATIC,
            HIDDEN, MODIFIABLE, SORT_KEY, SORT_ORDER, SIGNATURE, UNIDIRECTIONAL);

    private final Class attributeClass;

    private TypeAttributeType(Class<?> attributeClass) {
        assert attributeClass != null;
        this.attributeClass = attributeClass;
    }

    public boolean verifyAttribute(Object attribute) {
        return attribute != null && attributeClass.equals(attribute.getClass());
    }

    public Object defaultValue(TypeAttribute.Map map) {
        switch(this) {
            case SORT_ORDER: return Order.ASC;
            default: return null;
        }
    }

}
