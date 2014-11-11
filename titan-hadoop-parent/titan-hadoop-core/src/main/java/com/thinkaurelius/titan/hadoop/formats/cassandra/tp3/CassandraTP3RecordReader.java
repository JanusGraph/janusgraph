package com.thinkaurelius.titan.hadoop.formats.cassandra.tp3;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.hadoop.formats.util.GiraphRecordReader;
import com.thinkaurelius.titan.hadoop.formats.util.TitanVertexDeserializer;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphComputeVertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class CassandraTP3RecordReader extends RecordReader<NullWritable, GiraphComputeVertex> {

    private final GiraphRecordReader recordReader;

    public CassandraTP3RecordReader(TitanVertexDeserializer graph, RecordReader<StaticBuffer, Iterable<Entry>> rr) {
        recordReader = new GiraphRecordReader(graph, rr);
    }

    @Override
    public void initialize(final InputSplit inputSplit, final TaskAttemptContext context) throws IOException, InterruptedException {
        recordReader.initialize(inputSplit, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return recordReader.nextKeyValue();
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return recordReader.getCurrentKey();
    }

    @Override
    public GiraphComputeVertex getCurrentValue() throws IOException, InterruptedException {
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
