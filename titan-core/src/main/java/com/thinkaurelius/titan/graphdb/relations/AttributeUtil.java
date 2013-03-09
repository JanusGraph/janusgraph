package com.thinkaurelius.titan.graphdb.relations;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.attribute.Interval;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class AttributeUtil {

    public static final Object verifyAttributeQuery(TitanKey key, Object attribute) {
        if (attribute==null) return attribute;
        else if (attribute instanceof Interval) {
            Comparable start = (Comparable) verifyAttributeQuery(key,((Interval)attribute).getStart());
            Comparable end = (Comparable) verifyAttributeQuery(key,((Interval)attribute).getEnd());
            return new Interval(start,end);
        } else return verifyAttribute(key,attribute);
    }


    public static final Object verifyAttribute(TitanKey key, Object attribute) {
        attribute = prepareAttribute(attribute, key.getDataType());
        checkAttributeType(key, attribute);
        return attribute;
    }

    private static final Object prepareAttribute(Object attribute, Class<?> datatype) {
        Preconditions.checkNotNull(attribute, "Attribute cannot be null");
        if (!datatype.equals(Object.class)) {
            if (attribute instanceof Integer && datatype.equals(Long.class)) {
                attribute = Long.valueOf((Integer) attribute);
            } else if (attribute instanceof Number && datatype.equals(Double.class)) {
                attribute = ((Number)attribute).doubleValue();
            } else if (attribute instanceof Number && datatype.equals(Float.class)) {
                attribute = ((Number)attribute).floatValue();
            }
        }
        return attribute;
    }



    private static final void checkAttributeType(TitanKey key, Object attribute) {
        Class<?> datatype = key.getDataType();
        if (!datatype.equals(Object.class)) {
            Preconditions.checkArgument(datatype.equals(attribute.getClass()),
                    "Value [%s] is not an instance of the expected data type for property key [%s]. Expected: %s, found: %s", attribute,
                    key.getName(), datatype, attribute.getClass());
        }
    }

}
