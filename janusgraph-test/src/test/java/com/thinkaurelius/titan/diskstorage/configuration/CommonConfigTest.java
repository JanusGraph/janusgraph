package com.thinkaurelius.titan.diskstorage.configuration;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;

import com.thinkaurelius.titan.diskstorage.util.time.Temporals;
import org.apache.commons.configuration.BaseConfiguration;
import org.junit.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
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

        for (ChronoUnit unit : Arrays.asList(ChronoUnit.NANOS, ChronoUnit.MICROS, ChronoUnit.MILLIS, ChronoUnit.SECONDS, ChronoUnit.MINUTES, ChronoUnit.HOURS, ChronoUnit.DAYS)) {
            base.setProperty("test", "100 " + unit.toString());
            Duration d = config.get("test",Duration.class);
            assertEquals(TimeUnit.NANOSECONDS.convert(100, Temporals.timeUnit(unit)), d.toNanos());
        }

        Map<ChronoUnit,String> mapping = ImmutableMap.of(ChronoUnit.MICROS,"us", ChronoUnit.DAYS,"d");
        for (Map.Entry<ChronoUnit,String> entry : mapping.entrySet()) {
            base.setProperty("test","100 " + entry.getValue());
            Duration d = config.get("test",Duration.class);
            assertEquals(TimeUnit.NANOSECONDS.convert(100, Temporals.timeUnit(entry.getKey())),d.toNanos());
        }


    }
}
