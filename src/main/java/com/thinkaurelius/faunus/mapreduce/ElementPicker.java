package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusElement;
import com.thinkaurelius.faunus.Tokens;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ElementPicker {

    public static String getPropertyAsString(final FaunusElement element, final String key) {
        if (key.equals(Tokens._ID) || key.equals(Tokens.ID))
            return element.getId().toString();
        else if (key.equals(Tokens._PROPERTIES))
            return element.getProperties().toString();
        else if (key.equals(Tokens.LABEL) && element instanceof FaunusEdge)
            return ((FaunusEdge) element).getLabel();
        else {
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
        else if (key.equals(Tokens._PROPERTIES))
            return element.getProperties();
        else if (key.equals(Tokens.LABEL) && element instanceof FaunusEdge)
            return ((FaunusEdge) element).getLabel();
        else {
            return element.getProperty(key);
        }
    }
}
