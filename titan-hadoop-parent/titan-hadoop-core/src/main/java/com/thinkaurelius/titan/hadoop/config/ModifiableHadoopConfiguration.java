package com.thinkaurelius.titan.hadoop.config;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.gremlin.structure.Direction;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

public class ModifiableHadoopConfiguration extends ModifiableConfiguration {

//    private static final ModifiableHadoopConfiguration IMMUTABLE_CFG_WITH_RESOURCES;
//
//    static {
//        Configuration immutable = DEFAULT_COMPAT.newImmutableConfiguration(new Configuration(true));
//        IMMUTABLE_CFG_WITH_RESOURCES = new ModifiableHadoopConfiguration(immutable);
//    }

    private final Configuration conf;

    private ModifiableHadoopConfiguration(ConfigNamespace root, Configuration c, Restriction restriction) {
        super(root, new HadoopConfiguration(c), restriction);
        this.conf = c;
    }

//    /**
//     * Returns a ModifiableHadoopConfiguration backed by a an immutable Hadoop Configuration with
//     * default resources loaded (e.g. the contents of core-site.xml, core-default.xml, mapred-site.xml, ...).
//     *
//     * Immutability is guaranteed by encapsulating the Hadoop Configuration in a forwarder class that
//     * throws exceptions on data modification attempts.  Reads are supported though.
//     *
//     * @return
//     */
//    public static ModifiableHadoopConfiguration immutableWithResources() {
//        return IMMUTABLE_CFG_WITH_RESOURCES;
//    }

    public static ModifiableHadoopConfiguration of(ConfigNamespace root, Configuration c) {
        Preconditions.checkNotNull(c);
        return new ModifiableHadoopConfiguration(root, c, Restriction.NONE);
    }

    public static ModifiableConfiguration subset(ConfigNamespace newRoot, ConfigNamespace prefixRoot, ModifiableHadoopConfiguration mc) {
        HadoopConfiguration prefixConf = new HadoopConfiguration(mc.getHadoopConfiguration(), ConfigElement.getPath(prefixRoot) + ".");
        return new ModifiableConfiguration(newRoot, prefixConf,  Restriction.NONE);
    }

    public Configuration getHadoopConfiguration() {
        return conf;
    }

    public ModifiableConfiguration getInputConf(ConfigNamespace root) {
        return subset(root, TitanHadoopConfiguration.TITAN_INPUT_KEYS, this);
    }
}
