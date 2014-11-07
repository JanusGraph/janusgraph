package com.thinkaurelius.titan.hadoop.formats.hbase;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class TitanHBaseVertexReader extends VertexReader {

    private final TitanHBaseRecordReader recordReader;

    public TitanHBaseVertexReader(TitanHBaseHadoopGraph graph, TableRecordReader tableReader, byte[] edgeStoreName) {
        recordReader = new TitanHBaseRecordReader(graph, tableReader, edgeStoreName);
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
