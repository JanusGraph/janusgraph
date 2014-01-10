package com.thinkaurelius.titan.graphdb.database.serialize;

import com.thinkaurelius.titan.core.attribute.FullDouble;
import com.thinkaurelius.titan.core.attribute.FullFloat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class AttributeUtil {

    private static final Logger log = LoggerFactory.getLogger(AttributeUtil.class);


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

    /**
     * Compares the two elements like {@link java.util.Comparator#compare(Object, Object)} but returns
     * null in case the two elements are not comparable.
     *
     * @param a
     * @param b
     * @return
     */
    public static final Integer compare(Object a, Object b) {
        if (a==b) return 0;
        if (a==null || b==null) return null;
        assert a!=null && b!=null;
        if (a instanceof Number && b instanceof Number) {
            Number an = (Number)a;
            Number bn = (Number)b;
            if (Double.isNaN(an.doubleValue()) || Double.isNaN(bn.doubleValue())) {
                if (Double.isNaN(an.doubleValue()) && Double.isNaN(bn.doubleValue())) return 0;
                else return null;
            } else {
                if (an.doubleValue()==bn.doubleValue()) {
                    // Long.compare(long,long) is only available since Java 1.7
                    //return Long.compare(an.longValue(),bn.longValue());
                    return Long.valueOf(an.longValue()).compareTo(Long.valueOf(bn.longValue()));
                } else {
                    return Double.compare(an.doubleValue(),bn.doubleValue());
                }
            }
        } else {
            try {
                return ((Comparable)a).compareTo(b);
            } catch (Throwable e) {
                log.debug("Could not compare elements: {} - {}",a,b);
                return null;
            }
        }
    }

}
