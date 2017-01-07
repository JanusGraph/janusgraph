package org.janusgraph.graphdb.database.management;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.IndexField;
import org.janusgraph.graphdb.types.IndexType;
import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusGraphIndexWrapper implements JanusGraphIndex {

    private final IndexType index;

    public JanusGraphIndexWrapper(IndexType index) {
        this.index = index;
    }

    IndexType getBaseIndex() {
        return index;
    }

    @Override
    public String name() {
        return index.getName();
    }

    @Override
    public String getBackingIndex() {
        return index.getBackingIndexName();
    }

    @Override
    public Class<? extends Element> getIndexedElement() {
        return index.getElement().getElementType();
    }

    @Override
    public PropertyKey[] getFieldKeys() {
        IndexField[] fields = index.getFieldKeys();
        PropertyKey[] keys = new PropertyKey[fields.length];
        for (int i = 0; i < fields.length; i++) {
            keys[i]=fields[i].getFieldKey();
        }
        return keys;
    }

    @Override
    public Parameter[] getParametersFor(PropertyKey key) {
        if (index.isCompositeIndex()) return new Parameter[0];
        return ((MixedIndexType)index).getField(key).getParameters();
    }

    @Override
    public boolean isUnique() {
        if (index.isMixedIndex()) return false;
        return ((CompositeIndexType)index).getCardinality()== Cardinality.SINGLE;
    }

    @Override
    public SchemaStatus getIndexStatus(PropertyKey key) {
        Preconditions.checkArgument(Sets.newHashSet(getFieldKeys()).contains(key),"Provided key is not part of this index: %s",key);
        if (index.isCompositeIndex()) return ((CompositeIndexType)index).getStatus();
        else return ((MixedIndexType)index).getField(key).getStatus();
    }

    @Override
    public boolean isCompositeIndex() {
        return index.isCompositeIndex();
    }

    @Override
    public boolean isMixedIndex() {
        return index.isMixedIndex();
    }

    @Override
    public String toString() {
        return name();
    }

}

