package com.thinkaurelius.titan.hadoop.formats.hbase;

import com.tinkerpop.gremlin.giraph.structure.io.GiraphGremlinInputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

public class TitanHBaseVertexInputFormat extends VertexInputFormat implements GiraphGremlinInputFormat {

    private final TitanHBaseInputFormat inputFormat;

    public TitanHBaseVertexInputFormat() {
        inputFormat = new TitanHBaseInputFormat();
    }

    @Override
    public Class<InputFormat> getInputFormatClass() {
        return (Class)TitanHBaseInputFormat.class;
    }

    @Override
    public List<InputSplit> getSplits(JobContext context, int minSplitCountHint) throws IOException, InterruptedException {
        inputFormat.setConf(context.getConfiguration());
        return inputFormat.getSplits(context);
    }

    @Override
    public VertexReader createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
        try {
            inputFormat.createRecordReader(split, context);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        VertexReader reader = new TitanHBaseVertexReader(inputFormat.getGraph(), inputFormat.getTableReader(), inputFormat.getEdgeStoreFamily());
        try {
            reader.initialize(split, context);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        return reader;
    }
}
