package com.thinkaurelius.titan.hadoop.config;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Direction;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

public class ModifiableHadoopConfiguration extends ModifiableConfiguration {

    private static final ModifiableHadoopConfiguration IMMUTABLE_CFG_WITH_RESOURCES;

    static {
        Configuration immutable = DEFAULT_COMPAT.newImmutableConfiguration(new Configuration(true));
        IMMUTABLE_CFG_WITH_RESOURCES = new ModifiableHadoopConfiguration(immutable);
    }

    private final Configuration conf;

    private volatile Boolean trackPaths;
    private volatile Boolean trackState;

    public ModifiableHadoopConfiguration() {
        this(new Configuration());
    }

    public ModifiableHadoopConfiguration(Configuration c) {
        super(TitanHadoopConfiguration.ROOT_NS, new HadoopConfiguration(c), Restriction.NONE);
        this.conf = c;
    }

    /**
     * Returns a ModifiableHadoopConfiguration backed by a an immutable Hadoop Configuration with
     * default resources loaded (e.g. the contents of core-site.xml, core-default.xml, mapred-site.xml, ...).
     *
     * Immutability is guaranteed by encapsulating the Hadoop Configuration in a forwarder class that
     * throws exceptions on data modification attempts.  Reads are supported though.
     *
     * @return
     */
    public static ModifiableHadoopConfiguration immutableWithResources() {
        return IMMUTABLE_CFG_WITH_RESOURCES;
    }

    public static ModifiableHadoopConfiguration withoutResources() {
        return new ModifiableHadoopConfiguration(new Configuration(false));
    }

    public static ModifiableHadoopConfiguration of(Configuration c) {
        Preconditions.checkNotNull(c);
        return new ModifiableHadoopConfiguration(c);
    }

    public Configuration getHadoopConfiguration() {
        return conf;
    }

    @Override
    public <O> O get(ConfigOption<O> option, String... umbrellaElements) {
        if (TitanHadoopConfiguration.PIPELINE_TRACK_PATHS == option) {
            // Double writing this from concurrent threads is fine, mutex is overkill
            Boolean b = trackPaths;
            if (null == b) {
                b = (Boolean)super.get(option, umbrellaElements);
                trackPaths = b;
            }
            return (O)b;
        } else if (TitanHadoopConfiguration.PIPELINE_TRACK_STATE == option) {
            Boolean b = trackState;
            if (null == b) {
                b = (Boolean) super.get(option, umbrellaElements);
                trackState = b;
            }
            return (O) b;
        } else {
            return super.get(option, umbrellaElements);
        }
    }

    @Override
    public<O> ModifiableConfiguration set(ConfigOption<O> option, O value, String... umbrellaElements) {
        if (TitanHadoopConfiguration.PIPELINE_TRACK_PATHS == option) {
            trackPaths = null;
        } else if (TitanHadoopConfiguration.PIPELINE_TRACK_STATE == option) {
            trackState = null;
        }
        return super.set(option, value, umbrellaElements);
    }


    public void setAllOutput(Map<ConfigElement.PathIdentifier, Object> entries) {
        ModifiableConfiguration out = getOutputConf();
        for (Map.Entry<ConfigElement.PathIdentifier,Object> entry : entries.entrySet()) {
            Preconditions.checkArgument(entry.getKey().element.isOption());
            out.set((ConfigOption) entry.getKey().element, entry.getValue(), entry.getKey().umbrellaElements);
        }
    }

    public void setAllInput(Map<ConfigElement.PathIdentifier, Object> entries) {
        ModifiableConfiguration in = getInputConf();
        for (Map.Entry<ConfigElement.PathIdentifier,Object> entry : entries.entrySet()) {
            Preconditions.checkArgument(entry.getKey().element.isOption());
            in.set((ConfigOption) entry.getKey().element, entry.getValue(), entry.getKey().umbrellaElements);
        }
    }

    public Class<?> getClass(ConfigOption<String> opt, Class<?> cls) {
        return conf.getClass(ConfigElement.getPath(opt), cls);
    }

    public <T> Class<? extends T> getClass(ConfigOption<String> opt,  Class<? extends T> defaultValue, Class<T> iface) {
        return conf.getClass(ConfigElement.getPath(opt), defaultValue, iface);
    }

    public void setClass(ConfigOption<String> opt, Class<?> cls, Class<?> iface) {
        conf.setClass(ConfigElement.getPath(opt), cls, iface);
    }

    public ModifiableConfiguration getInputConf(ConfigNamespace root) {
        HadoopConfiguration inconf = new HadoopConfiguration(this.conf, ConfigElement.getPath(TitanHadoopConfiguration.INPUT_CONF_NS) + ".");
        return new ModifiableConfiguration(root, inconf,  Restriction.NONE);
    }

    public ModifiableConfiguration getInputConf() {
        return getInputConf(GraphDatabaseConfiguration.ROOT_NS);
    }

    public ModifiableConfiguration getOutputConf(ConfigNamespace root) {
        HadoopConfiguration outconf = new HadoopConfiguration(this.conf, ConfigElement.getPath(TitanHadoopConfiguration.OUTPUT_CONF_NS) + ".");
        return new ModifiableConfiguration(root, outconf, Restriction.NONE);
    }

    public ModifiableConfiguration getOutputConf() {
        return getOutputConf(GraphDatabaseConfiguration.ROOT_NS);
    }

    // Hack to support deprecation of the old edge copy dir option
    public Direction getEdgeCopyDirection() {
        if (has(TitanHadoopConfiguration.INPUT_EDGE_COPY_DIRECTION))
            return get(TitanHadoopConfiguration.INPUT_EDGE_COPY_DIRECTION);
        if (has(TitanHadoopConfiguration.INPUT_EDGE_COPY_DIR))
            return get(TitanHadoopConfiguration.INPUT_EDGE_COPY_DIR);

        return TitanHadoopConfiguration.INPUT_EDGE_COPY_DIRECTION.getDefaultValue();
    }
}
