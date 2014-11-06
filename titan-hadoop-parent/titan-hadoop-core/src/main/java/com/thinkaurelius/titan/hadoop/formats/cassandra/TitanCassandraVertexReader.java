package com.thinkaurelius.titan.hadoop.formats.cassandra;

import org.apache.cassandra.hadoop.ColumnFamilyRecordReader;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class TitanCassandraVertexReader extends VertexReader {

    private final TitanCassandraRecordReader recordReader;

    public TitanCassandraVertexReader(TitanCassandraHadoopGraph graph, ColumnFamilyRecordReader cfrr) {
        recordReader = new TitanCassandraRecordReader(graph, cfrr);
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
