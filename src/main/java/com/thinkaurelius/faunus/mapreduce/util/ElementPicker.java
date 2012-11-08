package com.thinkaurelius.faunus.mapreduce.util;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusElement;
import com.thinkaurelius.faunus.Tokens;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ElementPicker {

    protected ElementPicker() {
    }

    public static String getPropertyAsString(final FaunusElement element, final String key) {
        if (key.equals(Tokens._ID) || key.equals(Tokens.ID))
            return element.getId().toString();
        else if (key.equals(Tokens._PROPERTIES)) {
            final Map<String, Object> properties = new HashMap<String, Object>();
            for (final Map.Entry<String, Object> entry : element.getProperties().entrySet()) {
                properties.put(entry.getKey(), entry.getValue());
            }
            properties.put(Tokens._ID, element.getId());
            return properties.toString();
        } else if (key.equals(Tokens.LABEL) && element instanceof FaunusEdge) {
            return ((FaunusEdge) element).getLabel();
        } else {
            final Object value = element.getProperty(key);
            if (null != value)
                return value.toString();
            else
                return Tokens.NULL;
        }
    }

    public static Object getProperty(final FaunusElement element, final String key) {
        if (key.equals(Tokens._ID) || key.equals(Tokens.ID))
            return element.getId();
        else if (key.equals(Tokens._PROPERTIES)) {
            final Map<String, Object> properties = new HashMap<String, Object>();
            for (final Map.Entry<String, Object> entry : element.getProperties().entrySet()) {
                properties.put(entry.getKey(), entry.getValue());
            }
            properties.put(Tokens._ID, element.getId());
            return properties;
        } else if (key.equals(Tokens.LABEL) && element instanceof FaunusEdge) {
            return ((FaunusEdge) element).getLabel();
        } else {
            return element.getProperty(key);
        }
    }
}
