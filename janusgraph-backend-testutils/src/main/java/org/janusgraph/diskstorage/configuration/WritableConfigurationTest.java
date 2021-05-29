// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.configuration;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class WritableConfigurationTest {

    private WriteConfiguration config;

    public abstract WriteConfiguration getConfig();

    @BeforeEach
    public void setup() {
        config = getConfig();
    }

    @AfterEach
    public void cleanup() {
        config.close();
    }

    @Test
    public void configTest() {
        config.set("test.key","world");
        config.set("test.bar", 100);
        config.set("storage.xyz", true);
        config.set("storage.abc", Boolean.FALSE);
        config.set("storage.duba", new String[]{"x", "y"});
        config.set("times.60m", Duration.ofMinutes(60));
        config.set("obj", new Object()); // necessary for AbstractConfiguration.getSubset
        assertAll(
            () -> assertEquals("world", config.get("test.key", String.class)),
            () -> assertEquals(ImmutableSet.of("test.key", "test.bar"), Sets.newHashSet(config.getKeys("test"))),
            // () -> assertEquals(ImmutableSet.of("test.key", "test.bar", "test.baz"), Sets.newHashSet(config.getKeys("test"))),
            () -> assertEquals(ImmutableSet.of("storage.xyz", "storage.duba", "storage.abc"),
                Sets.newHashSet(config.getKeys("storage"))),
            () -> assertEquals(100,config.get("test.bar",Integer.class).intValue()),
            // () -> assertEquals(1,config.get("test.baz",Integer.class).intValue()),
            () -> assertEquals(true, config.get("storage.xyz", Boolean.class)),
            () -> assertEquals(false, config.get("storage.abc", Boolean.class)),
            () -> assertArrayEquals(new String[]{"x", "y"}, config.get("storage.duba", String[].class)),
            () -> assertEquals(Duration.ofMinutes(60), config.get("times.60m", Duration.class)),
            () -> assertTrue(Object.class.isAssignableFrom(config.get("obj", Object.class).getClass()))
        );
    }
}
