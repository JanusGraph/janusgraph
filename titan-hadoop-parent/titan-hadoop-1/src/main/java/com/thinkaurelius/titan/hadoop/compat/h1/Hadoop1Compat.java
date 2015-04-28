package com.thinkaurelius.titan.hadoop.compat.h1;

import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.thinkaurelius.titan.graphdb.configuration.TitanConstants;
import com.thinkaurelius.titan.hadoop.config.job.JobClasspathConfigurer;
import com.thinkaurelius.titan.hadoop.formats.cassandra.CassandraBinaryInputFormat;
import com.thinkaurelius.titan.hadoop.scan.HadoopVertexScanMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.thinkaurelius.titan.hadoop.compat.HadoopCompat;

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
        return new DistCacheConfigurer("titan-hadoop-core-" + TitanConstants.VERSION + ".jar");
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
