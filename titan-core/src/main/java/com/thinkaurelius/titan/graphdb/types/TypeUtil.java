package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.graphdb.internal.ElementCategory;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.query.graph.GraphCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.tinkerpop.blueprints.Element;

import java.util.ArrayList;
import java.util.Arrays;
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

    public static boolean hasSimpleInternalVertexKeyIndex(TitanKey key) {
        InternalRelationType type = (InternalRelationType)key;
        for (IndexType index : type.getKeyIndexes()) {
            if (index.getElement()== ElementCategory.VERTEX && index.isInternalIndex()) {
                InternalIndexType iIndex = (InternalIndexType)index;
                if (iIndex.getFields().length==1) {
                    assert iIndex.getFields()[0].getFieldKey().equals(key);
                    return true;
                }
            }
        }
        return false;
    }

    public static InternalRelationType getBaseType(InternalRelationType type) {
        InternalRelationType baseType = type.getBaseType();
        if (baseType == null) return type;
        else return baseType;
    }

    public static Set<TitanKey> getIndexedKeys(IndexType index) {
        Set<TitanKey> s = Sets.newHashSet();
        for (IndexField f : index.getFields()) {
            s.add(f.getFieldKey());
        }
        return s;
    }

    public static List<InternalIndexType> getUniqueIndexes(TitanKey key) {
        List<InternalIndexType> indexes = new ArrayList<InternalIndexType>();
        for (IndexType index : ((InternalRelationType)key).getKeyIndexes()) {
            if (index.isInternalIndex()) {
                InternalIndexType iIndex = (InternalIndexType)index;
                assert index.indexesKey(key);
                if (iIndex.getCardinality()==Cardinality.SINGLE) {
                    assert iIndex.getElement()==ElementCategory.VERTEX;
                    indexes.add(iIndex);
                }
            }
        }
        return indexes;
    }


}
