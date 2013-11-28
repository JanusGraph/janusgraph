package com.thinkaurelius.titan.util.system;

import org.apache.commons.configuration.Configuration;

import java.util.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ConfigurationUtil {

    private static final char CONFIGURATION_SEPARATOR = '.';

    public static List<String> getUnqiuePrefixes(Configuration config) {
        Set<String> nameSet = new HashSet<String>();
        List<String> names = new ArrayList<String>();
        Iterator<String> keyiter = config.getKeys();
        while (keyiter.hasNext()) {
            String key = keyiter.next();
            int pos = key.indexOf(CONFIGURATION_SEPARATOR);
            if (pos > 0) {
                String name = key.substring(0, pos);
                if (nameSet.add(name)) {
                    names.add(name);
                }
            }
        }
        return names;
    }

}
