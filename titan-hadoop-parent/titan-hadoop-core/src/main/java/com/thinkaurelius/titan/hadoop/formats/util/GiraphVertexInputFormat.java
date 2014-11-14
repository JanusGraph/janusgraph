package com.thinkaurelius.titan.hadoop.formats.util;

import com.thinkaurelius.titan.hadoop.formats.cassandra.giraph.CassandraGiraphInputFormat;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphComputeVertex;
import com.tinkerpop.gremlin.giraph.structure.io.GiraphGremlinInputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

public class GiraphVertexInputFormat extends VertexInputFormat implements GiraphGremlinInputFormat {

    private final InputFormat<NullWritable, GiraphComputeVertex> inputFormat;

    public GiraphVertexInputFormat(InputFormat<NullWritable, GiraphComputeVertex> inputFormat) {
        this.inputFormat = inputFormat;
    }

    // This is required by TP3's GiraphGremlinInputFormat
    // It's not documented, but TP3 requires the InputFormat to have generic types <NullWritable, GiraphComputeVertex>
    @Override
    public Class<InputFormat> getInputFormatClass() {
        return (Class)CassandraGiraphInputFormat.class;
    }

    @Override
    public List<InputSplit> getSplits(JobContext context, int minSplitCountHint) throws IOException, InterruptedException {
        ((Configurable)inputFormat).setConf(context.getConfiguration());
        return inputFormat.getSplits(context);
    }

    @Override
    public VertexReader createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
        VertexReader reader;
        try {
            reader = new GiraphVertexReader(inputFormat.createRecordReader(split, context));
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

