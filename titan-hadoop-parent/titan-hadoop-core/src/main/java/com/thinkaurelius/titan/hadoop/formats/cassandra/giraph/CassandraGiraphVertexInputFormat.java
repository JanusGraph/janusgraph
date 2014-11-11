package com.thinkaurelius.titan.hadoop.formats.cassandra.giraph;

import com.thinkaurelius.titan.hadoop.formats.cassandra.tp3.CassandraTP3InputFormat;
import com.tinkerpop.gremlin.giraph.structure.io.GiraphGremlinInputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

public class CassandraGiraphVertexInputFormat extends VertexInputFormat implements GiraphGremlinInputFormat {

    private final CassandraTP3InputFormat inputFormat;

    public CassandraGiraphVertexInputFormat() {
        inputFormat = new CassandraTP3InputFormat();
    }

    // This is required by TP3's GiraphGremlinInputFormat
    // It's not documented, but TP3 requires the InputFormat to have generic types <NullWritable, GiraphComputeVertex>
    @Override
    public Class<InputFormat> getInputFormatClass() {
        return (Class)CassandraTP3InputFormat.class;
    }

    @Override
    public List<InputSplit> getSplits(JobContext context, int minSplitCountHint) throws IOException, InterruptedException {
        inputFormat.setConf(context.getConfiguration());
        return inputFormat.getSplits(context);
    }

    @Override
    public VertexReader createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
        VertexReader reader;
        try {
            reader = new CassandraGiraphVertexReader(inputFormat.createRecordReader(split, context));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        try {
            reader.initialize(split, context);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        return reader;
    }
}
