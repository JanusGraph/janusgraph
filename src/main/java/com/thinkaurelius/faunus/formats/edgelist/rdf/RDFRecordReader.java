package com.thinkaurelius.faunus.formats.edgelist.rdf;

import com.thinkaurelius.faunus.FaunusElement;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RDFRecordReader extends RecordReader<NullWritable, FaunusElement> {

    private RDFBlueprintsHandler handler;
    private LineRecordReader lineRecordReader;

    private FaunusElement element;

    public RDFRecordReader(final Configuration configuration) throws IOException {
        this.lineRecordReader = new LineRecordReader();
        this.handler = new RDFBlueprintsHandler(configuration);
    }

    @Override
    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
        this.lineRecordReader.initialize(genericSplit, context);

    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (this.handler.hasNext()) {
            this.element = this.handler.next();
            return true;
        }
        while (this.lineRecordReader.nextKeyValue()) {
            this.handler.parse(this.lineRecordReader.getCurrentValue().toString());
            if (this.handler.hasNext()) {
                this.element = this.handler.next();
                return true;
            }
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public FaunusElement getCurrentValue() {
        return this.element;
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