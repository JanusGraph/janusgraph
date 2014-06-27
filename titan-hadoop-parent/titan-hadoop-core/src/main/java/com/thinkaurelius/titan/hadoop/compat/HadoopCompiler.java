package com.thinkaurelius.titan.hadoop.compat;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import com.thinkaurelius.titan.hadoop.Tokens;


// This interface must stay API compatible with both Hadoop1 and Hadoop2
public interface HadoopCompiler extends Configurable, Tool {

    public static final String TESTING = Tokens.makeNamespace(HadoopCompiler.class) + ".testing";

    public void addMapReduce(final Class<? extends Mapper> mapper,
            final Class<? extends Reducer> combiner,
            final Class<? extends Reducer> reducer,
            final Class<? extends WritableComparable> mapOutputKey,
            final Class<? extends WritableComparable> mapOutputValue,
            final Class<? extends WritableComparable> reduceOutputKey,
            final Class<? extends WritableComparable> reduceOutputValue,
            final Configuration configuration);

    public void addMapReduce(final Class<? extends Mapper> mapper,
            final Class<? extends Reducer> combiner,
            final Class<? extends Reducer> reducer,
            final Class<? extends WritableComparator> comparator,
            final Class<? extends WritableComparable> mapOutputKey,
            final Class<? extends WritableComparable> mapOutputValue,
            final Class<? extends WritableComparable> reduceOutputKey,
            final Class<? extends WritableComparable> reduceOutputValue,
            final Configuration configuration);

    public void addMap(final Class<? extends Mapper> mapper,
            final Class<? extends WritableComparable> mapOutputKey,
            final Class<? extends WritableComparable> mapOutputValue,
            final Configuration configuration);

    public void completeSequence();

    public void composeJobs() throws IOException;

    public int run(final String[] args) throws Exception;
}
