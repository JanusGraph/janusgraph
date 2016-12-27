package com.thinkaurelius.titan.hadoop.compat.h2;

import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.thinkaurelius.titan.graphdb.configuration.TitanConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import com.thinkaurelius.titan.hadoop.compat.HadoopCompat;
import com.thinkaurelius.titan.hadoop.config.job.JobClasspathConfigurer;

import java.io.IOException;

public class Hadoop2Compat implements HadoopCompat {

    // Hadoop Configuration keys
    private static final String CFG_SPECULATIVE_MAPS = "mapreduce.map.speculative";
    private static final String CFG_SPECULATIVE_REDUCES = "mapreduce.reduce.speculative";
    private static final String CFG_JOB_JAR = "mapreduce.job.jar";

    @Override
    public TaskAttemptContext newTask(Configuration c, TaskAttemptID t) {
        return new TaskAttemptContextImpl(c, t);
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
    public void incrementContextCounter(TaskInputOutputContext context,
            String group, String name, long incr) {
        context.getCounter(group, name).increment(incr);
    }

    @Override
    public long getContextCounter(TaskInputOutputContext context, String group, String name) {
        return context.getCounter(group, name).getValue();
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
        /* The exact jar passed in here doesn't really matter, as long as it exists.
         * This argument becomes the MapReduce job jar.  It isn't even really
         * necessary to set a job jar since we upload everything to the Hadoop
         * Distributed Cache, but setting one avoids a warning from JobClient:
         *     "No job jar file set.  User classes may not be found."
         */
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
        return new Hadoop2CountersScanMetrics(c);
    }

    @Override
    public String getJobFailureString(Job j) {
        try {
            JobStatus js = j.getStatus();
            return String.format("state=%s, failureinfo=%s", js.getState(), js.getFailureInfo());
        } catch (IOException e) {
            throw new TitanException(e);
        } catch (InterruptedException e) {
            throw new TitanException(e);
        }
    }
}
