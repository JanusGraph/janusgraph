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

package org.janusgraph.testutil;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.util.time.Temporals;
import org.janusgraph.util.system.ConfigurationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

/**
 * A central mechanism for overriding graph configuration parameters during
 * unit and integration testing. For example, an EC2 m1.medium used for
 * continuous integration might be much slower than SSD-backed personal
 * workstation, so much that increasing various backend-related timeouts from
 * their default values is warranted to prevent avoidable timeout failures. Not
 * intended for use in production.
 */
public class TestGraphConfigs {

    public static final String ENV_OVERRIDE_FILE = "JANUSGRAPH_CONFIG";

    private static final CommonsConfiguration overrides;

    private static final Logger log = LoggerFactory.getLogger(TestGraphConfigs.class);

    static {
        String overridesFile = System.getenv(ENV_OVERRIDE_FILE);

        CommonsConfiguration o = null;

        if (null != overridesFile) {
            if (!new File(overridesFile).isFile()) {
                log.warn("Graph configuration overrides file {} does not exist or is not an ordinary file", overridesFile);
            } else {
                try {
                    PropertiesConfiguration cc = ConfigurationUtil.loadPropertiesConfig(overridesFile);
                    o = new CommonsConfiguration(cc);
                    log.info("Loaded configuration from file {}", overridesFile);
                } catch (ConfigurationException e) {
                    log.error("Unable to load graph configuration from file {}", overridesFile, e);
                }
            }
        }

        overrides = o;
    }


    public static void applyOverrides(final WriteConfiguration base) {
        if (null == overrides)
            return;

        for (String k : overrides.getKeys(null)) {
            base.set(k, overrides.get(k, Object.class));
        }
    }

    public static long getTTL(TimeUnit u) {
        final long sec = 10L;
        long l = u.convert(sec, TimeUnit.SECONDS);
        // Check that a narrowing cast to int will not overflow, in case a test decides to try it.
        Preconditions.checkState(Integer.MIN_VALUE <= l && Integer.MAX_VALUE >= l,
                "Test TTL %d is too large to express as an integer in %s", sec, u);
        return l;
    }

    // This is used as a timeout argument to a loop that only sleeps briefly and checks
    // for convergence much more often than the timeout argument; it can safely be set
    // high without delaying successful tests
    public static long getSchemaConvergenceTime(ChronoUnit u) {
        final long sec = 60L;
        long l = Temporals.timeUnit(u).convert(sec, TimeUnit.SECONDS);
        // Check that a narrowing cast to int will not overflow, in case a test decides to try it.
        Preconditions.checkState(Integer.MIN_VALUE <= l && Integer.MAX_VALUE >= l,
                "Schema convergence time %d is too large to express as an integer in %s", sec, u);
        return l;
    }

}
