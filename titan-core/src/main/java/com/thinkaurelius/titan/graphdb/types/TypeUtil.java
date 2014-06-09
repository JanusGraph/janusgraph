package com.thinkaurelius.titan.graphdb.types;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.graphdb.internal.ElementCategory;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.tinkerpop.blueprints.Direction;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TypeUtil {

    public static boolean hasSimpleInternalVertexKeyIndex(TitanRelation rel) {
        if (!(rel instanceof TitanProperty)) return false;
        else return hasSimpleInternalVertexKeyIndex((TitanProperty)rel);
    }

    public static boolean hasSimpleInternalVertexKeyIndex(TitanProperty prop) {
        return hasSimpleInternalVertexKeyIndex(prop.getPropertyKey());
    }

    public static boolean hasSimpleInternalVertexKeyIndex(PropertyKey key) {
        InternalRelationType type = (InternalRelationType)key;
        for (IndexType index : type.getKeyIndexes()) {
            if (index.getElement()== ElementCategory.VERTEX && index.isInternalIndex()) {
                if (index.indexesKey(key)) return true;
//                InternalIndexType iIndex = (InternalIndexType)index;
//                if (iIndex.getFieldKeys().length==1) {
//                    assert iIndex.getFieldKeys()[0].getFieldKey().equals(key);
//                    return true;
//                }
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
        Set<PropertyKey> s = Sets.newHashSet();
        for (IndexField f : index.getFieldKeys()) {
            s.add(f.getFieldKey());
        }
        return s;
    }

    public static List<InternalIndexType> getUniqueIndexes(PropertyKey key) {
        List<InternalIndexType> indexes = new ArrayList<InternalIndexType>();
        for (IndexType index : ((InternalRelationType)key).getKeyIndexes()) {
            if (index.isInternalIndex()) {
                InternalIndexType iIndex = (InternalIndexType)index;
                assert index.indexesKey(key);
                if (iIndex.getCardinality()== Cardinality.SINGLE) {
                    assert iIndex.getElement()==ElementCategory.VERTEX;
                    indexes.add(iIndex);
                }
            }
        }
        return indexes;
    }

    public static ConsistencyModifier getConsistencyModifier(SchemaSource schema) {
        SchemaSource.Entry entry = Iterables.getFirst(schema.getRelated(TypeDefinitionCategory.CONSISTENCY_MODIFIER, Direction.OUT), null);
        if (entry==null) return ConsistencyModifier.DEFAULT;
        else return entry.getSchemaType().getDefinition().getValue(TypeDefinitionCategory.CONSISTENCY_LEVEL,ConsistencyModifier.class);
    }


}
