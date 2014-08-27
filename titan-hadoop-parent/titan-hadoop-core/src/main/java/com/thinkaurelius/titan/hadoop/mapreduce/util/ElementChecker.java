package com.thinkaurelius.titan.hadoop.mapreduce.util;

import com.thinkaurelius.titan.hadoop.FaunusElement;
import com.tinkerpop.blueprints.Compare;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ElementChecker {

    private final String key;
    private final Compare compare;
    private final Object[] values;

    public ElementChecker(final String key, final Compare compare, final Object... values) {
        this.key = key;
        this.compare = compare;
        this.values = values;
    }

    public boolean isLegal(final FaunusElement element) {
        Object elementValue = ElementPicker.getProperty(element, this.key);
        if (elementValue instanceof Number)
            elementValue = ((Number) elementValue).floatValue();

        boolean legal = false;
        for (final Object value : this.values) {
            legal = legal || compare.evaluate(elementValue, value);
        }
        return legal;
    }
}
