package com.thinkaurelius.titan.util.system;

import com.thinkaurelius.titan.core.util.ReflectiveConfigOptionLoader;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConfigurationLint {

    private static final Logger log =
            LoggerFactory.getLogger(ConfigurationLint.class);

    public static void main(String args[]) throws IOException {
        if (1 != args.length) {
            System.err.println("Usage: ConfigurationLint titan.properties");
            System.err.println("  Reads the supplied config file from disk and checks for unknown options.");
            System.exit(1);
        }

        log.info("Checking " + args[0]);
        Status s = validate(args[0]);
        if (0 == s.errors) {
            log.info(s.toString());
        } else {
            log.warn(s.toString());
        }
        System.exit(Math.min(s.errors, 127));
    }

    public static Status validate(String filename) throws IOException {
        Properties p = new Properties();
        FileInputStream fis = new FileInputStream(filename);
        p.load(fis);
        fis.close();

        int keys = 0;
        int errors = 0;
        for (Object k : p.keySet()) {
            String key = k.toString();
            keys++;
            try {
                ConfigElement.parse(GraphDatabaseConfiguration.ROOT_NS, key);
                log.info("Validated option: {}", key);
            } catch (Throwable t) {
                log.warn(String.format("Unknown configuration key: %s", key));
                errors++;
            }
        }

        return new Status(keys, errors);
    }

    public static class Status {
        private final int total;
        private final int errors;

        public Status(int total, int errors) {
            this.total = total;
            this.errors = errors;
        }

        public String toString() {
            if (0 == errors) {
                return String.format("[ConfigurationLint.Status: OK: %d keys validated]", total);
            } else {
                return String.format("[ConfigurationLint.Status: WARNING: %d unknown keys out of %d total]", errors, total);
            }
        }
    }
}
