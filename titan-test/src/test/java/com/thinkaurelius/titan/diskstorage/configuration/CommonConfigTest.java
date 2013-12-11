package com.thinkaurelius.titan.diskstorage.configuration;

import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import org.apache.commons.configuration.BaseConfiguration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class CommonConfigTest extends WritableConfigurationTest {

    @Override
    public WriteConfiguration getConfig() {
        return new CommonsConfiguration(new BaseConfiguration());
    }
}
