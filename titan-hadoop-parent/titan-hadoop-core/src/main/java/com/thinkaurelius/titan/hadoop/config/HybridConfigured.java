package com.thinkaurelius.titan.hadoop.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;

/**
 * Like Hadoop {@link Configured}, except that it maintains an additional
 * {@link BasicConfiguration} backed by the Hadoop {@code Configuration}. Reads
 * can go through either object, but writes can only be done through the
 * {@code Configuration}.
 *
 * @deprecated
 */
public class HybridConfigured extends Configured {

    protected ModifiableHadoopConfiguration titanConf;

    /**
     * Construct an empty HybridConfigured.
     */
    public HybridConfigured() {
        this(new Configuration());
    }

    /**
     * Construct a HybridConfigured with a clone of the supplied config.
     */
    public HybridConfigured(Configuration conf) {
        super(conf);
        titanConf = ModifiableHadoopConfiguration.of(conf);
    }

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        titanConf = ModifiableHadoopConfiguration.of(conf);
    }

    public ModifiableHadoopConfiguration getTitanConf() {
        return titanConf;
    }

    public void clearConfiguration() {
        setConf(new Configuration());
    }
}
