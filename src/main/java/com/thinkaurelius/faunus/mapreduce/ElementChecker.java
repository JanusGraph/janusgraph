package com.thinkaurelius.faunus.mapreduce;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Query;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ElementChecker {

    private final String key;
    private final Query.Compare compare;
    private final Object value;
    private final boolean nullIsWildcard;

    public ElementChecker(final String key, final Query.Compare compare, final Object value, final boolean nullIsWildcard) {
        this.key = key;
        this.compare = compare;
        this.value = value;
        this.nullIsWildcard = nullIsWildcard;
    }

    public boolean isLegal(final Element element) {
        Object elementValue = element.getProperty(this.key);
        if (elementValue instanceof Number)
            elementValue = ((Number) elementValue).floatValue();

        switch (this.compare) {
            case EQUAL:
                if (null == elementValue)
                    return this.value == null;
                return elementValue.equals(this.value);
            case NOT_EQUAL:
                if (null == elementValue)
                    return this.value != null;
                return !elementValue.equals(this.value);
            case GREATER_THAN:
                if (null == elementValue || this.value == null)
                    return this.nullIsWildcard;
                return ((Comparable) elementValue).compareTo(this.value) >= 1;
            case LESS_THAN:
                if (null == elementValue || this.value == null)
                    return this.nullIsWildcard;
                return ((Comparable) elementValue).compareTo(this.value) <= -1;
            case GREATER_THAN_EQUAL:
                if (null == elementValue || this.value == null)
                    return this.nullIsWildcard;
                return ((Comparable) elementValue).compareTo(this.value) >= 0;
            case LESS_THAN_EQUAL:
                if (null == elementValue || this.value == null)
                    return this.nullIsWildcard;
                return ((Comparable) elementValue).compareTo(this.value) <= 0;
            default:
                throw new IllegalArgumentException("Invalid state as no valid filter was provided");
        }
    }
}
