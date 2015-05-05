package com.thinkaurelius.titan.hadoop.config;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.hadoop.conf.Configuration;

public class ModifiableHadoopConfiguration extends ModifiableConfiguration {

    private final Configuration conf;

    private ModifiableHadoopConfiguration(ConfigNamespace root, Configuration c, Restriction restriction) {
        super(root, new HadoopConfiguration(c), restriction);
        this.conf = c;
    }

    public static ModifiableHadoopConfiguration of(ConfigNamespace root, Configuration c) {
        Preconditions.checkNotNull(c);
        return new ModifiableHadoopConfiguration(root, c, Restriction.NONE);
    }

    public static ModifiableConfiguration prefixView(ConfigNamespace newRoot, ConfigNamespace prefixRoot,
                                                     ModifiableHadoopConfiguration mc) {
        HadoopConfiguration prefixConf = new HadoopConfiguration(mc.getHadoopConfiguration(),
                ConfigElement.getPath(prefixRoot, true) + ".");
        return new ModifiableConfiguration(newRoot, prefixConf,  Restriction.NONE);
    }

    public Configuration getHadoopConfiguration() {
        return conf;
    }

    public ModifiableConfiguration getTitanGraphConf() {
        return prefixView(GraphDatabaseConfiguration.ROOT_NS, TitanHadoopConfiguration.GRAPH_CONFIG_KEYS, this);
    }
}
