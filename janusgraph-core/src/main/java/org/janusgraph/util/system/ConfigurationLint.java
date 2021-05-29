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

package org.janusgraph.util.system;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

public class ConfigurationLint {

    private static final Logger log =
            LoggerFactory.getLogger(ConfigurationLint.class);

    public static void main(String[] args) throws IOException {
        if (1 != args.length) {
            System.err.println("Usage: ConfigurationLint janusgraph.properties");
            System.err.println("  Reads the supplied config file from disk and checks for unknown options.");
            System.exit(1);
        }

        log.info("Checking " + LoggerUtil.sanitizeAndLaunder(args[0]));
        Status s = validate(args[0]);
        if (0 == s.errors) {
            log.info(s.toString());
        } else {
            log.warn(s.toString());
        }
        System.exit(Math.min(s.errors, 127));
    }

    public static Status validate(String filename) throws IOException {
        try (final FileInputStream fis = new FileInputStream(filename)) {
            new Properties().load(fis);
        }

        final PropertiesConfiguration apc;
        try {
            apc = ConfigurationUtil.loadPropertiesConfig(filename);
        } catch (ConfigurationException e) {
            throw new IOException(e);
        }

//        new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,
//                , BasicConfiguration.Restriction.NONE);

        Iterator<String> iterator = apc.getKeys();

        int totalKeys = 0;
        int keysVerified = 0;

        while (iterator.hasNext()) {
            totalKeys++;
            String key = iterator.next();
            String value = apc.getString(key);
            try {
                ConfigElement.PathIdentifier pid = ConfigElement.parse(GraphDatabaseConfiguration.ROOT_NS, key);
                // ConfigElement shouldn't return null; failure here probably relates to janusgraph-core, not the file
                Preconditions.checkNotNull(pid);
                Preconditions.checkNotNull(pid.element);
                if (!pid.element.isOption()) {
                    log.warn("Config key {} is a namespace (only options can be keys)", key);
                    continue;
                }
                final ConfigOption<?> opt;
                try {
                    opt = (ConfigOption<?>) pid.element;
                } catch (RuntimeException re) {
                    // This shouldn't happen given the preceding check, but catch it anyway
                    log.warn("Config key {} maps to the element {}, but it could not be cast to an option",
                            key, pid.element, re);
                    continue;
                }
                try {
                    Object o = new CommonsConfiguration(apc).get(key, opt.getDatatype());
                    opt.verify(o);
                    keysVerified++;
                } catch (RuntimeException re) {
                    log.warn("Config key {} is recognized, but its value {} could not be validated",
                            key, value /*, re*/);
                    log.debug("Validation exception on {}={} follows", key, value, re);
                }
            } catch (RuntimeException re) {
                log.warn("Unknown config key {}", key);
            }
        }


        return new Status(totalKeys, totalKeys - keysVerified);
    }

    public static class Status {
        private final int total;
        private final int errors;

        public Status(int total, int errors) {
            this.total = total;
            this.errors = errors;
        }

        public int getTotalSettingCount() {
            return total;
        }

        public int getErrorSettingCount() {
            return errors;
        }

        public String toString() {
            if (0 == errors) {
                return String.format("[ConfigurationLint.Status: OK: %d settings validated]", total);
            } else {
                return String.format("[ConfigurationLint.Status: WARNING: %d settings failed to validate out of %d total]", errors, total);
            }
        }
    }
}
