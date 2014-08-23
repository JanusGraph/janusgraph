package com.thinkaurelius.titan.hadoop.compat.h2;

import com.thinkaurelius.titan.graphdb.configuration.TitanConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import com.thinkaurelius.titan.hadoop.HadoopGraph;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompat;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompiler;
import com.thinkaurelius.titan.hadoop.config.job.JobClasspathConfigurer;

public class Hadoop2Compat implements HadoopCompat {

    // Hadoop Configuration keys
    private static final String CFG_SPECULATIVE_MAPS = "mapreduce.map.speculative";
    private static final String CFG_SPECULATIVE_REDUCES = "mapreduce.reduce.speculative";
    private static final String CFG_JOB_JAR = "mapreduce.job.jar";

    @Override
    public HadoopCompiler newCompiler(HadoopGraph g) {
        return new Hadoop2Compiler(g);
    }

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
    public void incrementContextCounter(TaskInputOutputContext context, Enum<?> counter,
            long incr) {
        context.getCounter(counter).increment(incr);
    }

    @Override
    public Configuration getContextConfiguration(TaskAttemptContext context) {
        return context.getConfiguration();
    }

    @Override
    public long getCounter(MapReduceDriver counters, Enum<?> e) {
        return counters.getCounters().findCounter(e).getValue();
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
}
