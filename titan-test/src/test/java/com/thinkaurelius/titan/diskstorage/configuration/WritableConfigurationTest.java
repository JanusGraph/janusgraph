package com.thinkaurelius.titan.diskstorage.configuration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

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
        config.set("storage.xyz", true);
        config.set("storage.duba", new String[]{"x", "y"});
        assertEquals("world", config.get("test.key", String.class));
        assertEquals(ImmutableSet.of("test.key", "test.bar"), Sets.newHashSet(config.getKeys("test")));
        assertEquals(ImmutableSet.of("storage.xyz", "storage.duba"),Sets.newHashSet(config.getKeys("storage")));
        assertEquals(100,config.get("test.bar",Integer.class).intValue());
        assertEquals(true,config.get("storage.xyz",Boolean.class).booleanValue());
        assertTrue(Arrays.equals(new String[]{"x", "y"},config.get("storage.duba",String[].class)));
    }


}
