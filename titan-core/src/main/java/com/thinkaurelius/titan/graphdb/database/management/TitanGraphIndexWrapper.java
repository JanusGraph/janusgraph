package com.thinkaurelius.titan.graphdb.database.management;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.schema.Parameter;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.graphdb.types.ExternalIndexType;
import com.thinkaurelius.titan.graphdb.types.IndexField;
import com.thinkaurelius.titan.graphdb.types.IndexType;
import com.thinkaurelius.titan.graphdb.types.InternalIndexType;
import com.tinkerpop.blueprints.Element;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanGraphIndexWrapper implements TitanGraphIndex {

    private final IndexType index;

    public TitanGraphIndexWrapper(IndexType index) {
        this.index = index;
    }

    IndexType getBaseIndex() {
        return index;
    }

    @Override
    public String getName() {
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
        if (index.isInternalIndex()) return new Parameter[0];
        return ((ExternalIndexType)index).getField(key).getParameters();
    }

    @Override
    public boolean isUnique() {
        if (index.isExternalIndex()) return false;
        return ((InternalIndexType)index).getCardinality()== Cardinality.SINGLE;
    }
}
