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
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.graphdb.database.management.ModifierType;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.JanusGraphSchemaCategory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class TypeUtil {

    public static boolean hasSimpleInternalVertexKeyIndex(JanusGraphRelation rel) {
        return rel instanceof JanusGraphVertexProperty && hasSimpleInternalVertexKeyIndex((JanusGraphVertexProperty) rel);
    }

    public static void checkTypeName(JanusGraphSchemaCategory category, String name) {
        switch (category) {
            case EDGELABEL:
            case VERTEXLABEL:
                if (name == null) throw Element.Exceptions.labelCanNotBeNull();
                if (StringUtils.isBlank(name)) throw Element.Exceptions.labelCanNotBeEmpty();
                break;
            case PROPERTYKEY:
                if (name == null) throw Property.Exceptions.propertyKeyCanNotBeNull();
                if (StringUtils.isBlank(name)) throw Property.Exceptions.propertyKeyCanNotBeEmpty();
                break;
            case GRAPHINDEX:
                Preconditions.checkArgument(StringUtils.isNotBlank(name),"Index name cannot be empty: %s",name);
                break;
            default: throw new AssertionError(category);
        }
    }

    public static boolean hasSimpleInternalVertexKeyIndex(JanusGraphVertexProperty prop) {
        return hasSimpleInternalVertexKeyIndex(prop.propertyKey());
    }

    public static boolean hasSimpleInternalVertexKeyIndex(PropertyKey key) {
        InternalRelationType type = (InternalRelationType)key;
        for (IndexType index : type.getKeyIndexes()) {
            if (index.getElement()== ElementCategory.VERTEX && index.isCompositeIndex()) {
                if (index.indexesKey(key)) return true;
            }
        }
        return false;
    }

    public static InternalRelationType getBaseType(InternalRelationType type) {
        InternalRelationType baseType = type.getBaseType();
        if (baseType == null) return type;
        else return baseType;
    }

    public static Set<PropertyKey> getIndexedKeys(IndexType index) {
        IndexField[] fieldKeys = index.getFieldKeys();
        Set<PropertyKey> s = new HashSet<>(fieldKeys.length);
        for (IndexField f : fieldKeys) {
            s.add(f.getFieldKey());
        }
        return s;
    }

    public static List<CompositeIndexType> getUniqueIndexes(PropertyKey key) {
        final List<CompositeIndexType> indexes = new ArrayList<>();
        for (IndexType index : ((InternalRelationType)key).getKeyIndexes()) {
            if (index.isCompositeIndex()) {
                CompositeIndexType iIndex = (CompositeIndexType)index;
                assert index.indexesKey(key);
                if (iIndex.getCardinality()== Cardinality.SINGLE) {
                    assert iIndex.getElement()==ElementCategory.VERTEX;
                    indexes.add(iIndex);
                }
            }
        }
        return indexes;
    }

    public static boolean hasAnyIndex(PropertyKey key) {
        InternalRelationType type = (InternalRelationType) key;
        return !Iterables.isEmpty(type.getKeyIndexes()) ||
                Iterables.size(type.getRelationIndexes())>1; //The type itself is also returned as an index
    }

    private static <T> T getTypeModifier(final SchemaSource schema,
                                         final ModifierType modifierType,
                                         final T defaultValue) {
        for (SchemaSource.Entry entry : schema.getRelated(TypeDefinitionCategory.TYPE_MODIFIER, Direction.OUT)) {
            T value = entry.getSchemaType().getDefinition().getValue(modifierType.getCategory());
            if (null != value) {
                return value;
            }
        }
        return defaultValue;
    }



    public static ConsistencyModifier getConsistencyModifier(SchemaSource schema) {
        return getTypeModifier(schema, ModifierType.CONSISTENCY, ConsistencyModifier.DEFAULT);
    }

    public static int getTTL(final SchemaSource schema) {
        return getTypeModifier(schema, ModifierType.TTL, 0);
    }
}
