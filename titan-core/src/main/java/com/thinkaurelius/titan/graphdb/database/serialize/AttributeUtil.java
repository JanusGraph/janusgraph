package com.thinkaurelius.titan.graphdb.database.serialize;

import com.thinkaurelius.titan.core.attribute.FullDouble;
import com.thinkaurelius.titan.core.attribute.FullFloat;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class AttributeUtil {

    public static final boolean isWholeNumber(Number n) {
        return isWholeNumber(n.getClass());
    }

    public static final boolean isDecimal(Number n) {
        return isDecimal(n.getClass());
    }

    public static final boolean isWholeNumber(Class<?> clazz) {
        return clazz.equals(Long.class) || clazz.equals(Integer.class) ||
                clazz.equals(Short.class) || clazz.equals(Byte.class);
    }

    public static final boolean isDecimal(Class<?> clazz) {
        return clazz.equals(Double.class) || clazz.equals(Float.class) ||
                clazz.equals(FullDouble.class) || clazz.equals(FullFloat.class);
    }

    public static final boolean isString(Object o) {
        return isString(o.getClass());
    }

    public static final boolean isString(Class<?> clazz) {
        return clazz.equals(String.class);
    }

}
