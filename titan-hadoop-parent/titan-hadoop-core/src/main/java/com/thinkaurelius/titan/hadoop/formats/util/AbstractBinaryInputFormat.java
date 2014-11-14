package com.thinkaurelius.titan.hadoop.formats.util;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;

public abstract class AbstractBinaryInputFormat extends InputFormat<StaticBuffer, Iterable<Entry>> implements Configurable {

    protected Configuration hadoopConf;
    protected ModifiableConfiguration inputConf;

    @Override
    public void setConf(final Configuration config) {

        ModifiableHadoopConfiguration faunusConf = ModifiableHadoopConfiguration.of(TitanHadoopConfiguration.SCAN_NS, config);
        this.inputConf = faunusConf.getInputConf(GraphDatabaseConfiguration.ROOT_NS);
        this.hadoopConf = config;
    }

    @Override
    public Configuration getConf() {
        return hadoopConf;
    }
}
