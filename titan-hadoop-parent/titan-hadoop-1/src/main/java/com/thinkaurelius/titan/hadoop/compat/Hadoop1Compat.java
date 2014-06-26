package com.thinkaurelius.titan.hadoop.compat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import com.thinkaurelius.titan.hadoop.HadoopGraph;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompat;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompiler;

public class Hadoop1Compat implements HadoopCompat {

    private static final String MAPREDUCE_SPECULATIVE_MAPS = "mapred.map.tasks.speculative.execution";
    private static final String MAPREDUCE_SPECULATIVE_REDUCES = "mapred.reduce.tasks.speculative.execution";

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
        return MAPREDUCE_SPECULATIVE_MAPS;
    }

    @Override
    public String getSpeculativeReduceConfigKey() {
        return MAPREDUCE_SPECULATIVE_REDUCES;
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
}
