package com.thinkaurelius.titan.hadoop.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration.Restriction;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.ReadConfiguration;

/**
 * Like Hadoop {@link Configured}, except that it maintains an additional
 * {@link BasicConfiguration} backed by the Hadoop {@code Configuration}. Reads
 * can go through either object, but writes can only be done through the
 * {@code Configuration}.
 *
 */
public class HybridConfigured extends Configured {

    private BasicConfiguration titanConf;

    /**
     * Construct an empty HybridConfigured.
     */
    public HybridConfigured() {
        super(new Configuration());
    }

    /**
     * Construct a HybridConfigured with a clone of the supplied config.
     */
    public HybridConfigured(Configuration conf) {
        super(conf);
    }

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        Preconditions.checkNotNull(conf);
        ReadConfiguration rc = new HadoopReadConfiguration(getConf());
        titanConf = new BasicConfiguration(TitanHadoopConfiguration.ROOT_NS, rc, Restriction.NONE);
    }

    public BasicConfiguration getTitanConf() {
        return titanConf;
    }

    public Class<?> getConfClass(ConfigOption<String> opt, Class<?> cls) {
        return getConf().getClass(ConfigElement.getPath(opt), cls);
    }

    public void setConfClass(ConfigOption<String> opt, Class<?> cls, Class<?> iface) {
        getConf().setClass(ConfigElement.getPath(opt), cls, iface);
    }
}
