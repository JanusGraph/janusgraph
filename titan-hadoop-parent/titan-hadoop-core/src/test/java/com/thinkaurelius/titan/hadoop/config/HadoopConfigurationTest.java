package com.thinkaurelius.titan.hadoop.config;

import com.thinkaurelius.titan.diskstorage.configuration.WritableConfigurationTest;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import org.apache.hadoop.conf.Configuration;

public class HadoopConfigurationTest extends WritableConfigurationTest {

    @Override
    public WriteConfiguration getConfig() {
        return new HadoopConfiguration(new Configuration(false));
    }

}
