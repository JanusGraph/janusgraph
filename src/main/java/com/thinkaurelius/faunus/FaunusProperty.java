package com.thinkaurelius.faunus;

import com.google.common.base.Preconditions;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FaunusProperty {

    private final FaunusType type;
    private final Object value;


    public FaunusProperty(FaunusType type, Object value) {
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

}
