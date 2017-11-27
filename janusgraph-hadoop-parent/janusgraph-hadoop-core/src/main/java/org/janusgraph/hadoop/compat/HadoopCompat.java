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

package org.janusgraph.hadoop.compat;

import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.janusgraph.hadoop.config.job.JobClasspathConfigurer;

/**
 * This interface encapsulates both API and bytecode-level
 * IncompatibleClassChanges in Hadoop core. In theory, janusgraph-hadoop-core should
 * only touch parts of Hadoop core ABI that have remained stable since 1.0, and
 * everything else should be hidden behind the compat layer.
 * <p>
 * This interface is public, but should be considered unstable and likely to
 * change in the future as new Hadoop versions are released, or as
 * janusgraph-hadoop-core uses additional Hadoop features, or as bugs are discovered.
 * It's possible to write and use a third-party implementation, but be prepared
 * to update it when upgrading to a newer JanusGraph release.
 */
public interface HadoopCompat {

    /**
     * Instantiate a new TaskAttemptContext using the given attempt ID and configuration.
     *
     * @param c configuration
     * @param t task attempt ID
     * @return new context object
     */
    TaskAttemptContext newTask(Configuration c, TaskAttemptID t);

    /**
     * Return the Hadoop configuration key which takes a boolean value and
     * controls whether Hadoop will attempt speculative execution of mappers.
     *
     * @return string config key
     */
    String getSpeculativeMapConfigKey();

    /**
     * Return the Hadoop configuration key which takes a boolean value and
     * controls whether Hadoop will attempt speculative execution of reducers.
     *
     * @return string config key
     */
    String getSpeculativeReduceConfigKey();

    String getMapredJarConfigKey();

//    boolean runVertexScan(String vertexScanJobClass, Configuration jobConf) throws IOException, ClassNotFoundException, InterruptedException;

    /**
     * Add {@code increment} to the counter designated by {@code counter} on {@code context}.
     *
     * @param context Hadoop task IO context containing counter state
     * @param group the Hadoop counter group (heading under which the counter is displayed)
     * @param name the Hadoop counter name (the identifier for this counter within the group)
     * @param increment amount to add to the counter's current value
     */
    void incrementContextCounter(TaskInputOutputContext context, String group, String name, long increment);

    /**
     * Return the current value of counter designated by {@code counter} on {@code context}.
     *
     * @param context Hadoop task IO context containing counter state
     * @param group the Hadoop counter group (heading under which the counter is displayed)
     * @param name the Hadoop counter name (the identifier for this counter within the group)
     * @return current counter value
     */
    long getContextCounter(TaskInputOutputContext context, String group, String name);

    /**
     * Get configuration from the supplied task attempt context and return it.
     *
     * @param context Hadoop task attempt context
     * @return configuration on supplied {@code context}
     */
    Configuration getContextConfiguration(TaskAttemptContext context);

    /**
     * Get configuration from the supplied job context and return it.
     *
     * @param context Hadoop job context
     * @return configuration on supplied {@code context}
     */
    Configuration getJobContextConfiguration(JobContext context);

    /**
     * Construct a {@link org.janusgraph.hadoop.config.job.JobClasspathConfigurer}
     * that sets the MapReduce job jar config key to the supplied value.  The job jar
     * should contain Faunus's classes plus its entire dependency tree ("fat" jar).
     *
     * @param mapReduceJarPath path to the map reduce job jar
     * @return a configurer
     */
    JobClasspathConfigurer newMapredJarConfigurer(String mapReduceJarPath);

    /**
     * Construct a {@link org.janusgraph.hadoop.config.job.JobClasspathConfigurer}
     * that walks the classpath and adds all jars its finds to the Hadoop Jobs's
     * class paths via the Hadoop Distributed Cache.
     *
     * @return a configurer
     */
    JobClasspathConfigurer newDistCacheConfigurer();

    /**
     * Construct a {@link org.apache.hadoop.conf.Configuration} instance which throws
     * {@link java.lang.UnsupportedOperationException} on any attempt to modify its state,
     * but which forwards to its parameter all method calls that don't mutate state (i.e. reads).
     *
     * @param base the configuration to encapsulate behind an immutable forwarder class
     * @return an immutable forwarder class that encapsulates {@code base}
     */
    Configuration newImmutableConfiguration(Configuration base);

    ScanMetrics getMetrics(Counters c);

    String getJobFailureString(Job j);
}
