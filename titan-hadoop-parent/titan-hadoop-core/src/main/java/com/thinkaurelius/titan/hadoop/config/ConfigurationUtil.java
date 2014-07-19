package com.thinkaurelius.titan.hadoop.config;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;

import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration.Restriction;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

import java.util.Iterator;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ConfigurationUtil {

    public static BaseConfiguration extractConfiguration(final Configuration config, final String prefix) {
        return extractConfiguration(config, prefix, true);
    }

    public static BaseConfiguration extractConfiguration(final Configuration config, final String prefix, final boolean removeDelimChar) {
        final BaseConfiguration extract = new BaseConfiguration();
        final Iterator<Map.Entry<String, String>> itty = config.iterator();
        while (itty.hasNext()) {
            final Map.Entry<String, String> entry = itty.next();
            final String key = entry.getKey();
            final String value = entry.getValue();
            extract.setProperty(key, value);
            if (key.startsWith(prefix)) {
                if (removeDelimChar) {
                    extract.setProperty(key.substring(prefix.length() + 1), value);
                } else {
                    extract.setProperty(key.substring(prefix.length()), value);
                }
            }
        }
        return extract;
    }

    public static BasicConfiguration extractInputConfiguration(final Configuration config) {
        CommonsConfiguration cc = new CommonsConfiguration(extractConfiguration(config, ConfigElement.getPath(TitanHadoopConfiguration.INPUT_CONF_NS)));
        return new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS, cc, Restriction.NONE);
    }

    public static BasicConfiguration extractOutputConfiguration(final Configuration config) {
        CommonsConfiguration cc = new CommonsConfiguration(extractConfiguration(config, ConfigElement.getPath(TitanHadoopConfiguration.OUTPUT_CONF_NS)));
        return new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS, cc, Restriction.NONE);
    }

    public static BasicConfiguration convert(final Configuration config) {
        CommonsConfiguration cc = new CommonsConfiguration(extractConfiguration(config, "", false));
        return new BasicConfiguration(TitanHadoopConfiguration.ROOT_NS, cc, Restriction.NONE);
    }

    public static <T> T get(final Configuration config, final ConfigOption<T> opt) {
        CommonsConfiguration cc = new CommonsConfiguration(extractConfiguration(config, "", false));
        BasicConfiguration bc = new BasicConfiguration(TitanHadoopConfiguration.ROOT_NS, cc, Restriction.NONE);
        if (!bc.has(opt))
            return null;
        return bc.get(opt);
    }

    public static <T> void copyValue(final Configuration sink, BasicConfiguration source, ConfigOption<T> opt) {
        CommonsConfiguration cc = new CommonsConfiguration(extractConfiguration(sink, "", false));
        ModifiableConfiguration mc = new ModifiableConfiguration(TitanHadoopConfiguration.ROOT_NS, cc, Restriction.NONE);
        mc.set(opt, source.get(opt));
    }
}
