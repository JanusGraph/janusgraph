package com.thinkaurelius.titan.graphdb.types.indextype;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.graphdb.internal.ElementCategory;
import com.thinkaurelius.titan.graphdb.types.IndexField;
import com.thinkaurelius.titan.graphdb.types.IndexType;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory;
import com.thinkaurelius.titan.graphdb.types.TypeSource;

import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class IndexTypeWrapper implements IndexType {

    protected final TypeSource base;

    public IndexTypeWrapper(TypeSource base) {
        Preconditions.checkNotNull(base);
        this.base = base;
    }

    @Override
    public ElementCategory getElement() {
        return base.getDefinition().getValue(TypeDefinitionCategory.ELEMENT_CATEGORY,ElementCategory.class);
    }

    @Override
    public int hashCode() {
        return base.hashCode();
    }

    @Override
    public boolean equals(Object oth) {
        if (this==oth) return true;
        else if (oth==null || !getClass().isInstance(oth)) return false;
        IndexTypeWrapper other = (IndexTypeWrapper)oth;
        return base.equals(other.base);
    }

    @Override
    public String toString() {
        return "index" + base.getID();
    }

    private Map<TitanKey,IndexField> fieldMap = null;

    @Override
    public IndexField getField(TitanKey key) {
        if (fieldMap==null) {
            ImmutableMap.Builder<TitanKey,IndexField> b = ImmutableMap.builder();
            for (IndexField f : getFields()) b.put(f.getFieldKey(),f);
            fieldMap=b.build();
        }
        assert fieldMap!=null;
        return fieldMap.get(key);
    }

    @Override
    public boolean indexesKey(TitanKey key) {
        return getField(key)!=null;
    }

}
