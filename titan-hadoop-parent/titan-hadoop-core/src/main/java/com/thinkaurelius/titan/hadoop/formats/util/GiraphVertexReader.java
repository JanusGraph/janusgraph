package com.thinkaurelius.titan.hadoop.formats.util;

import com.tinkerpop.gremlin.giraph.process.computer.GiraphComputeVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class GiraphVertexReader extends VertexReader {

    private final RecordReader<NullWritable, GiraphComputeVertex> recordReader;

    public GiraphVertexReader(RecordReader<NullWritable, GiraphComputeVertex> recordReader) {
        this.recordReader = recordReader;
    }

    @Override
    public void initialize(final InputSplit inputSplit,
                           final TaskAttemptContext context) throws IOException, InterruptedException {
        recordReader.initialize(inputSplit, context);
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
        return recordReader.nextKeyValue();
    }

    @Override
    public Vertex getCurrentVertex() throws IOException, InterruptedException {
        return recordReader.getCurrentValue();
    }

    @Override
    public void close() throws IOException {
        recordReader.close();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return recordReader.getProgress();
    }
}
