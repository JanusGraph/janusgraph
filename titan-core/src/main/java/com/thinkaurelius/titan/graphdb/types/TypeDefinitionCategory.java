package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.internal.ElementCategory;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;

import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public enum TypeDefinitionCategory {

    //Relation Types
    HIDDEN(Boolean.class),
    SORT_KEY(long[].class),
    SORT_ORDER(Order.class),
    SIGNATURE(long[].class),
    MULTIPLICITY(Multiplicity.class),
    DATATYPE(Class.class),
    UNIDIRECTIONAL(Boolean.class),
    INVERTED_DIRECTION(Boolean.class),

    //Index Types
    ELEMENT_CATEGORY(ElementCategory.class),
    INDEX_CARDINALITY(Cardinality.class),
    IS_MAPPING(Boolean.class),
    INDEX_NAME(String.class),

    //Consistency Types
    CONSISTENCY_LEVEL(ConsistencyModifier.class),

    //Schema Edges
    RELATION_INDEX(),
    CONSISTENCY_MODIFIER(),
    INDEX_FIELD(RelationCategory.EDGE,Parameter[].class);

    static final Set<TypeDefinitionCategory> PROPERTY_KEY_DEFINITION_CATEGORIES = ImmutableSet.of(HIDDEN, SORT_KEY, SORT_ORDER, SIGNATURE, MULTIPLICITY, DATATYPE);
    static final Set<TypeDefinitionCategory> EDGE_LABEL_DEFINITION_CATEGORIES = ImmutableSet.of(HIDDEN, SORT_KEY, SORT_ORDER, SIGNATURE, MULTIPLICITY, UNIDIRECTIONAL, INVERTED_DIRECTION);

    public static final Set<TypeDefinitionCategory> SCHEMA_EDGE_DEFS = ImmutableSet.of(RELATION_INDEX,CONSISTENCY_MODIFIER,INDEX_FIELD);



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

}
