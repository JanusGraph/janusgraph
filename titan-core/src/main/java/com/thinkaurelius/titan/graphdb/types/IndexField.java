package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanKey;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IndexField {

    private final TitanKey key;

    IndexField(TitanKey key) {
        Preconditions.checkNotNull(key);
        this.key = key;
    }

    public TitanKey getFieldKey() {
        return key;
    }

    public static IndexField of(TitanKey key) {
        return new IndexField(key);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(key).toHashCode();
    }

    @Override
    public boolean equals(Object oth) {
        if (this==oth) return true;
        else if (oth==null || !getClass().isInstance(oth)) return false;
        IndexField other = (IndexField)oth;
        if (key==null) return key==other.key;
        else return key.equals(other.key);
    }

    @Override
    public String toString() {
        return "["+key.getName()+"]";
    }

}
