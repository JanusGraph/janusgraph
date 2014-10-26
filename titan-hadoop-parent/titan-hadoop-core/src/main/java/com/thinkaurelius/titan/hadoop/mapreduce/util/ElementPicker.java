package com.thinkaurelius.titan.hadoop.mapreduce.util;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.thinkaurelius.titan.core.TitanVertexProperty;
import com.thinkaurelius.titan.hadoop.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ElementPicker {

    protected ElementPicker() {
    }

    public static String getPropertyAsString(final FaunusElement element, final String key) {
        if (key.equals(Tokens._PROPERTIES)) {
            final ListMultimap<String, Object> properties = ArrayListMultimap.create();
            for (final TitanVertexProperty property : element.query().properties()) {
                properties.put(property.getType().name(), property.value());
            }
            properties.put(Tokens._ID, element.longId());
            if (element instanceof StandardFaunusEdge)
                properties.put(Tokens._LABEL, ((StandardFaunusEdge) element).getLabel());

            return properties.toString();
        } else {
            if (element instanceof FaunusVertex) {
                List values = new ArrayList();
                Iterables.addAll(values, ((FaunusVertex) element).getPropertyValues(key));
                if (values.size() == 0)
                    return Tokens.NULL;
                else if (values.size() == 1)
                    return values.iterator().next().toString();
                else {
                    return values.toString();
                }
            } else {
                final Object value = element.value(key);
                if (null != value)
                    return value.toString();
                else
                    return Tokens.NULL;
            }
        }
    }

    public static Object getProperty(final FaunusElement element, final String key) {
        if (key.equals(Tokens._PROPERTIES)) {
            final ListMultimap<String, Object> properties = ArrayListMultimap.create();
            for (final TitanVertexProperty property : element.query().properties()) {
                properties.put(property.getType().name(), property.value());
            }
            properties.put(Tokens._ID, element.longId());
            return properties;
        } else {
            if (element instanceof FaunusVertex) {
                List values = new ArrayList();
                Iterables.addAll(values, ((FaunusVertex) element).getPropertyValues(key));
                if (values.size() == 0)
                    return null;
                else if (values.size() == 1)
                    return values.iterator().next();
                else {
                    return values;
                }
            } else
                return element.value(key);
        }
    }
}
