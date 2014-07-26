package com.thinkaurelius.titan.hadoop.compat;

import java.io.File;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.graphdb.configuration.TitanConstants;
import com.thinkaurelius.titan.hadoop.HadoopGraph;

/**
 * Static companion class for {@link JobClasspathConfigurer} implementations.
 */
public class JobClasspathConfigurers {

    private static final Logger log =
            LoggerFactory.getLogger(JobClasspathConfigurers.class);

    // Hadoop 2 specific
    private static final String MAPRED_JAR = "mapred.jar";

    private static final String DEFAULT_MAPRED_JAR_FILENAME = "titan-hadoop-2-" + TitanConstants.VERSION + "-job.jar";

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

    public static JobClasspathConfigurer get(HadoopGraph graph) {

        // Check for Hadoop's mapred.jar config key; this takes highest precedence
        String mapredJar = graph.getConf().get(MAPRED_JAR, null);
        if (null != mapredJar) {
            log.info("Using configuration's mapred job jar: {}", mapredJar);
            return new MapredJarConfigurer(mapredJar);
        }

        // Check for job jar with default filename in some hardcoded directories
        for (String dir : POSSIBLE_MAPRED_JAR_DIRS) {
            String candidate = dir + File.separator + DEFAULT_MAPRED_JAR_FILENAME;
            if (new File(candidate).exists()) {
                log.info("Using mapred job jar found in {}: {}", dir, candidate);
                return new MapredJarConfigurer(candidate);
            }
        }

        // No job jar so far?  Propagate all jars on our classpath to the distributed cache.
        log.info("Uploading jars on classpath DistributedCache in lieu of a mapred job jar");
        return DistributedCacheConfigurer.fromJarsOnClasspath(DEFAULT_MAPRED_JAR_FILENAME);
    }
}
