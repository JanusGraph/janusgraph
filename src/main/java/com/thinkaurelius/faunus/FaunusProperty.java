package com.thinkaurelius.faunus;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FaunusProperty extends FaunusElement {

    private final FaunusType type;
    private final Object value;

    public FaunusProperty(FaunusType type, Object value) {
        this(0l,type,value);
    }

    public FaunusProperty(long id, String type, Object value) {
        this(id,FaunusType.DEFAULT_MANAGER.get(type),value);
    }

    public FaunusProperty(long id, FaunusType type, Object value) {
        super(id);
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(value);
        this.type = type;
        this.value = value;
    }

    public String getName() {
        return type.getName();
    }

    public FaunusType getType() {
        return type;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(type).append(value).toHashCode();
    }

    @Override
    public boolean equals(Object oth) {
        if (this==oth) return true;
        else if (oth==null || !getClass().isInstance(oth)) return false;
        FaunusProperty p = (FaunusProperty)oth;
        return type.equals(p.type) && value.equals(p.value);
    }

    @Override
    public String toString() {
        return type.toString() + "->" + value.toString();
    }

    //########### Make FaunusProperty not accept meta-properties (yet) #######

    @Override
    protected void initializeProperties() {
        throw new UnsupportedOperationException();
    }

}
