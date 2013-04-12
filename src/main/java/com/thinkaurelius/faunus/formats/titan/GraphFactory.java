package com.thinkaurelius.faunus.formats.titan;

import com.thinkaurelius.titan.core.TitanFactory;
import com.tinkerpop.blueprints.Graph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;
import java.util.Map;

/**
 * Converts a Faunus/Hadoop configuration file to a Titan configuration file.
 * For all Titan specific properties, the conversion chomps the Faunus prefix and provides to Titan's graph factory.
 *
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
            final Map.Entry<String, String> entry = itty.next();
            final String key = entry.getKey();
            final String value = entry.getValue();
            titanconfig.setProperty(key, value);
            if (key.startsWith(prefix)) {
                titanconfig.setProperty(key.substring(prefix.length() + 1), value);
            }
        }
        return titanconfig;
    }
}
