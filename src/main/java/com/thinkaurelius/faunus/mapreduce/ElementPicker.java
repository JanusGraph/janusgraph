package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.FaunusElement;
import com.thinkaurelius.faunus.Tokens;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ElementPicker {

    public static String getProperty(final FaunusElement element, final String key) {
        if (key.equals(Tokens._ID))
            return element.getId().toString();
        else if (key.equals(Tokens._PROPERTIES))
            return element.getProperties().toString();
        else {
            final Object value = element.getProperty(key);
            if (null != value)
                return value.toString();
            else
                return Tokens.NULL;
        }
    }
}
