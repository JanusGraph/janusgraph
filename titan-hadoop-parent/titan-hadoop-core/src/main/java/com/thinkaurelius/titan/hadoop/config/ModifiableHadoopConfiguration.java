package com.thinkaurelius.titan.hadoop.config;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public class ModifiableHadoopConfiguration extends ModifiableConfiguration {

    private final Configuration conf;

    public ModifiableHadoopConfiguration() {
        this(new Configuration());
    }

    public ModifiableHadoopConfiguration(Configuration c) {
        super(TitanHadoopConfiguration.ROOT_NS, new HadoopConfiguration(c), Restriction.NONE);
        this.conf = c;
    }

    public static ModifiableHadoopConfiguration of(Configuration c) {
        Preconditions.checkNotNull(c);
        return new ModifiableHadoopConfiguration(c);
    }

    public Configuration getHadoopConfiguration() {
        return conf;
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
}
