package com.thinkaurelius.titan.hadoop.compat.h1;

import com.thinkaurelius.titan.graphdb.configuration.TitanConstants;
import com.thinkaurelius.titan.hadoop.config.job.JobClasspathConfigurer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import com.thinkaurelius.titan.hadoop.HadoopGraph;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompat;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompiler;

public class Hadoop1Compat implements HadoopCompat {

    static final String CFG_SPECULATIVE_MAPS = "mapred.map.tasks.speculative.execution";
    static final String CFG_SPECULATIVE_REDUCES = "mapred.reduce.tasks.speculative.execution";
    static final String CFG_JOB_JAR = "mapred.jar";

    @Override
    public HadoopCompiler newCompiler(HadoopGraph g) {
        return new Hadoop1Compiler(g);
    }

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
    public void incrementContextCounter(TaskInputOutputContext context, Enum<?> counter, long incr) {
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
