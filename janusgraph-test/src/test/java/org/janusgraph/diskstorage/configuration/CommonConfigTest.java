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

import com.google.common.collect.ImmutableMap;
import org.apache.commons.configuration2.BaseConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.util.time.Temporals;
import org.janusgraph.util.system.ConfigurationUtil;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class CommonConfigTest extends WritableConfigurationTest {

    @Override
    public WriteConfiguration getConfig() {
        return new CommonsConfiguration(ConfigurationUtil.createBaseConfiguration());
    }

    @Test
    public void testDateParsing() {
        final BaseConfiguration base = ConfigurationUtil.createBaseConfiguration();
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
