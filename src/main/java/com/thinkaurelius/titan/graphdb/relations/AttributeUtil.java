package com.thinkaurelius.titan.graphdb.relations;

import com.google.common.base.Preconditions;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class AttributeUtil {

    public static final Object prepareAttribute(Object attribute, Class<?> datatype) {
        Preconditions.checkNotNull(attribute);
        if (!datatype.equals(Object.class)) {
            if (attribute instanceof Integer && datatype.equals(Long.class)) {
                attribute = Long.valueOf((Integer)attribute);
            } else if (attribute instanceof Float && datatype.equals(Double.class)) {
                attribute = Double.valueOf((Float)attribute);
            }
        }
        return attribute;
    }

}
