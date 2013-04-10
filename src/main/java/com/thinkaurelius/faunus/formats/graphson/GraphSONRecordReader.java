package com.thinkaurelius.faunus.formats.graphson;


import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphSONRecordReader extends RecordReader<NullWritable, FaunusVertex> {

    private boolean pathEnabled;
    private final LineRecordReader lineRecordReader;
    private FaunusVertex value = null;

    public GraphSONRecordReader() {
        this.lineRecordReader = new LineRecordReader();
    }

    @Override
    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
        this.lineRecordReader.initialize(genericSplit, context);
        this.pathEnabled = context.getConfiguration().getBoolean(FaunusCompiler.PATH_ENABLED, false);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (!this.lineRecordReader.nextKeyValue())
            return false;

        this.value = GraphSONUtility.fromJSON(this.lineRecordReader.getCurrentValue().toString());
        this.value.enablePath(this.pathEnabled);
        return true;
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public FaunusVertex getCurrentValue() {
        return this.value;
    }

    @Override
    public float getProgress() throws IOException {
        return this.lineRecordReader.getProgress();
    }

    @Override
    public synchronized void close() throws IOException {
        this.lineRecordReader.close();
    }
}