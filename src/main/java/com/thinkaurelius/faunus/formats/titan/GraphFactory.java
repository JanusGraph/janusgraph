package com.thinkaurelius.faunus.formats.titan;

import com.thinkaurelius.titan.core.TitanFactory;
import com.tinkerpop.blueprints.Graph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphFactory {

    public static Graph generateGraph(final Configuration config, final String prefix) {
        return TitanFactory.open(generateTitanConfiguration(config, prefix));
    }

    public static BaseConfiguration generateTitanConfiguration(final Configuration config, final String prefix) {
        final BaseConfiguration titanconfig = new BaseConfiguration();
        final Iterator<Map.Entry<String, String>> itty = config.iterator();
        while (itty.hasNext()) {
            Map.Entry<String, String> entry = itty.next();
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith(prefix)) {
                titanconfig.setProperty(key.substring(prefix.length() + 1), value);
            }
        }
        return titanconfig;
    }
}
