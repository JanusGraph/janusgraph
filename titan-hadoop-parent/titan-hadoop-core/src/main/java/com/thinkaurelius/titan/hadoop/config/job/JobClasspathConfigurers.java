package com.thinkaurelius.titan.hadoop.config.job;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

/**
 * Static companion class for {@link JobClasspathConfigurer} implementations.
 */
public class JobClasspathConfigurers {

    private static final Logger log =
            LoggerFactory.getLogger(JobClasspathConfigurers.class);

    private static final ImmutableList<String> POSSIBLE_MAPRED_JAR_DIRS;

    static {
        // Build a list of target, lib, ../target, ../lib
        // (except using the system's path separator character instead of hardcoding /)
        ImmutableList.Builder<String> b = ImmutableList.builder();
        for (String prefix : Arrays.asList("", ".." + File.separator)) {
            for (String dir : Arrays.asList("target", "lib")) {
                b.add(prefix + dir);
            }
        }
        POSSIBLE_MAPRED_JAR_DIRS = b.build();
    }

    public static JobClasspathConfigurer get(String configuredMapredJar, String defaultMapredJar) {

        // Check for Hadoop's mapred.jar config key; this takes highest precedence
        if (null != configuredMapredJar) {
            log.info("Using configuration's mapred job jar: {}", configuredMapredJar);
            return DEFAULT_COMPAT.newMapredJarConfigurer(configuredMapredJar);
        }

        // Check for job jar with default filename in some hardcoded directories
        for (String dir : POSSIBLE_MAPRED_JAR_DIRS) {
            String candidate = dir + File.separator + defaultMapredJar;
            if (new File(candidate).exists()) {
                log.info("Using mapred job jar found in {}: {}", dir, candidate);
                return DEFAULT_COMPAT.newMapredJarConfigurer(candidate);
            }
        }

        // No job jar so far?  Propagate all jars on our classpath to the distributed cache.
        log.info("Uploading jars on classpath DistributedCache in lieu of a mapred job jar");
        return DEFAULT_COMPAT.newDistCacheConfigurer();
    }
}
