package com.thinkaurelius.titan.hadoop.compat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import com.thinkaurelius.titan.hadoop.HadoopGraph;

/**
 * This interface encapsulates both API and bytecode-level
 * IncompatibleClassChanges in Hadoop core. In theory, titan-hadoop-core should
 * only touch parts of Hadoop core ABI that have remained stable since 1.0, and
 * everything else should be hidden behind the compat layer.
 * <p>
 * This interface is public, but should be considered unstable and likely to
 * change in the future as new Hadoop versions are released, or as
 * titan-hadoop-core uses additional Hadoop features, or as bugs are discovered.
 * It's possible to write and use a third-party implementation, but be prepared
 * to update it when upgrading to a newer Titan release.
 */
public interface HadoopCompat {

    /**
     * Instantiate and return a HadoopCompiler instance that uses the supplied
     * graph
     *
     * @param g data source/sink for the task compiler
     * @return new compiler
     */
    public HadoopCompiler newCompiler(HadoopGraph g);

    public TaskAttemptContext newTask(Configuration c, TaskAttemptID t);

    public String getSpeculativeMapConfigKey();

    public String getSpeculativeReduceConfigKey();

    public void incrementContextCounter(TaskInputOutputContext context, Enum<?> counter, long incr);

    public Configuration getContextConfiguration(TaskAttemptContext context);

    public Configuration getJobContextConfiguration(JobContext context);

    public long getCounter(MapReduceDriver counters, Enum<?> e);
}
