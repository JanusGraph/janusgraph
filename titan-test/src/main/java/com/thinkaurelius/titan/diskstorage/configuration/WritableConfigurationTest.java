package com.thinkaurelius.titan.diskstorage.configuration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class WritableConfigurationTest {

    private WriteConfiguration config;

    public abstract WriteConfiguration getConfig();

    @Before
    public void setup() {
        config = getConfig();
    }

    @After
    public void cleanup() {
        config.close();
    }

    @Test
    public void configTest() {
        config.set("test.key","world");
        config.set("test.bar", 100);
        // This does not actually work with KCVSConfiguration, but Titan doesn't need it anyway
        //config.set("test.baz", BigInteger.ONE);
        config.set("storage.xyz", true);
        config.set("storage.abc", Boolean.FALSE);
        config.set("storage.duba", new String[]{"x", "y"});
        config.set("enum", Thread.State.RUNNABLE);
        config.set("times.60m", new StandardDuration(60, TimeUnit.MINUTES));
        config.set("obj", new Object()); // necessary for AbstractConfiguration.getSubset
        assertEquals("world", config.get("test.key", String.class));
        assertEquals(ImmutableSet.of("test.key", "test.bar"), Sets.newHashSet(config.getKeys("test")));
        //assertEquals(ImmutableSet.of("test.key", "test.bar", "test.baz"), Sets.newHashSet(config.getKeys("test")));
        assertEquals(ImmutableSet.of("storage.xyz", "storage.duba", "storage.abc"),Sets.newHashSet(config.getKeys("storage")));
        assertEquals(100,config.get("test.bar",Integer.class).intValue());
        //assertEquals(1,config.get("test.baz",Integer.class).intValue());
        assertEquals(true,config.get("storage.xyz",Boolean.class).booleanValue());
        assertEquals(false,config.get("storage.abc",Boolean.class).booleanValue());
        assertTrue(Arrays.equals(new String[]{"x", "y"},config.get("storage.duba",String[].class)));
        assertEquals(Thread.State.RUNNABLE, config.get("enum", Thread.State.class));
        assertEquals(new StandardDuration(60, TimeUnit.MINUTES), config.get("times.60m", Duration.class));
        assertTrue(Object.class.isAssignableFrom(config.get("obj", Object.class).getClass()));
    }
}
