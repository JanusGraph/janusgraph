package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.FaunusElement;
import com.tinkerpop.blueprints.Query;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ElementChecker {

    private final String key;
    private final Query.Compare compare;
    private final Object[] values;
    private final boolean nullIsWildcard;

    public ElementChecker(final boolean nullIsWildcard, final String key, final Query.Compare compare, final Object... values) {
        this.key = key;
        this.compare = compare;
        this.values = values;
        this.nullIsWildcard = nullIsWildcard;
    }

    public boolean isLegal(final FaunusElement element) {
        Object elementValue = ElementPicker.getProperty(element, this.key);
        if (elementValue instanceof Number)
            elementValue = ((Number) elementValue).floatValue();

        switch (this.compare) {
            case EQUAL:
                if (null == elementValue) {
                    for (final Object value : values) {
                        if (null == value)
                            return true;
                    }
                    return false;
                } else {
                    for (final Object value : values) {
                        if (elementValue.equals(value))
                            return true;
                    }
                    return false;
                }
            case NOT_EQUAL:
                if (null == elementValue) {
                    for (final Object value : values) {
                        if (null != value)
                            return true;
                    }
                    return false;
                } else {
                    for (final Object value : values) {
                        if (!elementValue.equals(value))
                            return true;
                    }
                    return false;
                }
            case GREATER_THAN:
                if (null == elementValue) {
                    return this.nullIsWildcard;
                } else {
                    for (final Object value : values) {
                        if (((Comparable) elementValue).compareTo(value) >= 1)
                            return true;
                    }
                    return false;
                }
            case LESS_THAN:
                if (null == elementValue) {
                    return this.nullIsWildcard;
                } else {
                    for (final Object value : values) {
                        if (((Comparable) elementValue).compareTo(value) <= -1)
                            return true;
                    }
                    return false;
                }
            case GREATER_THAN_EQUAL:
                if (null == elementValue) {
                    return this.nullIsWildcard;
                } else {
                    for (final Object value : values) {
                        if (((Comparable) elementValue).compareTo(value) >= 0)
                            return true;
                    }
                    return false;
                }
            case LESS_THAN_EQUAL:
                if (null == elementValue) {
                    return this.nullIsWildcard;
                } else {
                    for (final Object value : values) {
                        if (((Comparable) elementValue).compareTo(value) <= 0)
                            return true;
                    }
                    return false;
                }
            default:
                throw new IllegalArgumentException("Invalid state as no valid filter was provided");
        }
    }
}
