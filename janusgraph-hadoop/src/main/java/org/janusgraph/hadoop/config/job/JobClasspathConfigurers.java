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

package org.janusgraph.hadoop.config.job;

import com.google.common.collect.ImmutableList;
import org.janusgraph.graphdb.configuration.JanusGraphConstants;
import org.janusgraph.hadoop.DistCacheConfigurer;
import org.janusgraph.hadoop.MapredJarConfigurer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;

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

    public static JobClasspathConfigurer get(String configuredMapReduceJar, String defaultMapReduceJar) {

        // Check for Hadoop's mapred.jar config key; this takes highest precedence
        if (null != configuredMapReduceJar) {
            log.info("Using configuration's mapred job jar: {}", configuredMapReduceJar);
            return new MapredJarConfigurer(configuredMapReduceJar);
        }

        // Check for job jar with default filename in some hardcoded directories
        for (String dir : POSSIBLE_MAPRED_JAR_DIRS) {
            String candidate = dir + File.separator + defaultMapReduceJar;
            if (new File(candidate).exists()) {
                log.info("Using mapred job jar found in {}: {}", dir, candidate);
                return new MapredJarConfigurer(candidate);
            }
        }

        // No job jar so far?  Propagate all jars on our classpath to the distributed cache.
        log.info("Uploading jars on classpath DistributedCache in lieu of a mapred job jar");
        return new DistCacheConfigurer("janusgraph-hadoop-" + JanusGraphConstants.VERSION + ".jar");
    }
}
