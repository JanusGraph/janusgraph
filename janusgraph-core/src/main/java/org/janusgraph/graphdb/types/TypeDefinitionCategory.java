// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.types;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.graphdb.database.management.ModifierType;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.internal.RelationCategory;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public enum TypeDefinitionCategory {

    //Relation Types
    INVISIBLE(Boolean.class),
    SORT_KEY(long[].class),
    SORT_ORDER(Order.class),
    SIGNATURE(long[].class),
    MULTIPLICITY(Multiplicity.class),
    DATATYPE(Class.class),
    UNIDIRECTIONAL(Direction.class),

    //General admin
    STATUS(SchemaStatus.class),

    //Index Types
    ELEMENT_CATEGORY(ElementCategory.class),
    INDEX_CARDINALITY(Cardinality.class),
    INTERNAL_INDEX(Boolean.class),
    BACKING_INDEX(String.class),
    INDEXSTORE_NAME(String.class),

    //Consistency Types
    CONSISTENCY_LEVEL(ConsistencyModifier.class),

    // type modifiers
    TTL(Integer.class),

    //Vertex Label
    PARTITIONED(Boolean.class),
    STATIC(Boolean.class),

    //Schema Edges
    RELATIONTYPE_INDEX(),
    TYPE_MODIFIER(),
    INDEX_FIELD(RelationCategory.EDGE,Parameter[].class),
    INDEX_SCHEMA_CONSTRAINT();

    public static final Set<TypeDefinitionCategory> PROPERTYKEY_DEFINITION_CATEGORIES = ImmutableSet.of(STATUS, INVISIBLE, SORT_KEY, SORT_ORDER, SIGNATURE, MULTIPLICITY, DATATYPE);
    public static final Set<TypeDefinitionCategory> EDGELABEL_DEFINITION_CATEGORIES = ImmutableSet.of(STATUS, INVISIBLE, SORT_KEY, SORT_ORDER, SIGNATURE, MULTIPLICITY, UNIDIRECTIONAL);
    public static final Set<TypeDefinitionCategory> INDEX_DEFINITION_CATEGORIES = ImmutableSet.of(STATUS, ELEMENT_CATEGORY,INDEX_CARDINALITY,INTERNAL_INDEX, BACKING_INDEX,INDEXSTORE_NAME);
    public static final Set<TypeDefinitionCategory> VERTEXLABEL_DEFINITION_CATEGORIES = ImmutableSet.of(PARTITIONED,STATIC);
    public static final Set<TypeDefinitionCategory> TYPE_MODIFIER_DEFINITION_CATEGORIES;

    static {
        ImmutableSet.Builder<TypeDefinitionCategory> builder = ImmutableSet.builder();
        for (ModifierType type : ModifierType.values()) {
            builder.add(type.getCategory());
        }
        TYPE_MODIFIER_DEFINITION_CATEGORIES = builder.build();
    }

    private final RelationCategory relationCategory;
    private final Class dataType;

    TypeDefinitionCategory() {
        this(RelationCategory.EDGE,null);
    }

    TypeDefinitionCategory(Class<?> dataType) {
        this(RelationCategory.PROPERTY, dataType);
    }

    TypeDefinitionCategory(RelationCategory relCat, Class<?> dataType) {
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
            case STATUS: return SchemaStatus.ENABLED;
            default: return null;
        }
    }

}
