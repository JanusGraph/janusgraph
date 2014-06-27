package com.thinkaurelius.titan.hadoop.compat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import com.thinkaurelius.titan.hadoop.HadoopGraph;

public class Hadoop2Compat implements HadoopCompat {

    // Hadoop Configuration keys
    private static final String CFG_SPECULATIVE_MAPS = "mapreduce.map.speculative";
    private static final String CFG_SPECULATIVE_REDUCES = "mapreduce.reduce.speculative";

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
}
