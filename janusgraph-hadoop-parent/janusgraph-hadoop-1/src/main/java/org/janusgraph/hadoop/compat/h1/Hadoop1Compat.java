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

package org.janusgraph.hadoop.compat.h1;

import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.graphdb.configuration.JanusGraphConstants;
import org.janusgraph.hadoop.config.job.JobClasspathConfigurer;
import org.janusgraph.hadoop.formats.cassandra.CassandraBinaryInputFormat;
import org.janusgraph.hadoop.scan.HadoopVertexScanMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import org.janusgraph.hadoop.compat.HadoopCompat;

import java.io.IOException;

public class Hadoop1Compat implements HadoopCompat {

    static final String CFG_SPECULATIVE_MAPS = "mapred.map.tasks.speculative.execution";
    static final String CFG_SPECULATIVE_REDUCES = "mapred.reduce.tasks.speculative.execution";
    static final String CFG_JOB_JAR = "mapred.jar";

    @Override
    public TaskAttemptContext newTask(Configuration c, TaskAttemptID t) {
        return new TaskAttemptContext(c, t);
    }

    @Override
    public String getSpeculativeMapConfigKey() {
        return CFG_SPECULATIVE_MAPS;
    }

    @Override
    public String getSpeculativeReduceConfigKey() {
        return CFG_SPECULATIVE_REDUCES;
    }

    @Override
    public String getMapredJarConfigKey() {
        return CFG_JOB_JAR;
    }

    @Override
    public long getContextCounter(TaskInputOutputContext context, String group, String name) {
        return context.getCounter(group, name).getValue();
    }

    @Override
    public void incrementContextCounter(TaskInputOutputContext context,
                                        String group, String name, long incr) {
        context.getCounter(group, name).increment(incr);
    }

    @Override
    public Configuration getContextConfiguration(TaskAttemptContext context) {
        return context.getConfiguration();
    }

    @Override
    public JobClasspathConfigurer newMapredJarConfigurer(String mapredJarPath) {
        return new MapredJarConfigurer(mapredJarPath);
    }

    @Override
    public JobClasspathConfigurer newDistCacheConfigurer() {
        return new DistCacheConfigurer("janusgraph-hadoop-core-" + JanusGraphConstants.VERSION + ".jar");
    }

    @Override
    public Configuration getJobContextConfiguration(JobContext context) {
        return context.getConfiguration();
    }

    @Override
    public Configuration newImmutableConfiguration(Configuration base) {
        return new ImmutableConfiguration(base);
    }

    @Override
    public ScanMetrics getMetrics(Counters c) {
        return new Hadoop1CountersScanMetrics(c);
    }

    @Override
    public String getJobFailureString(Job j) {
        return j.toString();
    }
}
