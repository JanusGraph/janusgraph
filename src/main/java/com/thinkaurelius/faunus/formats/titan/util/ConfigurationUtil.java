package com.thinkaurelius.faunus.formats.titan.util;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ConfigurationUtil {

    public static BaseConfiguration extractConfiguration(final Configuration config, final String prefix) {
        final BaseConfiguration extract = new BaseConfiguration();
        final Iterator<Map.Entry<String, String>> itty = config.iterator();
        while (itty.hasNext()) {
            final Map.Entry<String, String> entry = itty.next();
            final String key = entry.getKey();
            final String value = entry.getValue();
            extract.setProperty(key, value);
            if (key.startsWith(prefix)) {
                extract.setProperty(key.substring(prefix.length() + 1), value);
            }
        }
        return extract;
    }

}
