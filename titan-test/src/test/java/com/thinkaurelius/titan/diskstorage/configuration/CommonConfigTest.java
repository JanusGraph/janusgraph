package com.thinkaurelius.titan.diskstorage.configuration;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.core.attribute.Duration;
import org.apache.commons.configuration.BaseConfiguration;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class CommonConfigTest extends WritableConfigurationTest {

    @Override
    public WriteConfiguration getConfig() {
        return new CommonsConfiguration(new BaseConfiguration());
    }

    @Test
    public void testDateParsing() {
        BaseConfiguration base = new BaseConfiguration();
        CommonsConfiguration config = new CommonsConfiguration(base);

        for (TimeUnit unit : TimeUnit.values()) {
            base.setProperty("test","100 " + unit.toString());
            Duration d = config.get("test",Duration.class);
            assertEquals(unit,d.getNativeUnit());
            assertEquals(100,d.getLength(unit));
        }

        Map<TimeUnit,String> mapping = ImmutableMap.of(TimeUnit.MICROSECONDS,"us", TimeUnit.DAYS,"d");
        for (Map.Entry<TimeUnit,String> entry : mapping.entrySet()) {
            base.setProperty("test","100 " + entry.getValue());
            Duration d = config.get("test",Duration.class);
            assertEquals(entry.getKey(),d.getNativeUnit());
            assertEquals(100,d.getLength(entry.getKey()));
        }


    }
}
