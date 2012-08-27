package com.thinkaurelius.faunus.formats.rexster;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.graphson.GraphSONUtility;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

/**
 * Gets vertices from Rexster via the Gremlin Extension and a custom Gremlin script that iterates
 * vertices and converts them to the Faunus GraphSON that is understood by the Faunus GraphSONUtility.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RexsterRecordReader extends RecordReader<NullWritable, FaunusVertex> {

    private final RexsterConfiguration rexsterConf;
    private RexsterIterator rexsterIterator;

    private final NullWritable key = NullWritable.get();

    /**
     * The current vertex in the reader.
     */
    private FaunusVertex value = null;

    private long splitStart;
    private long splitEnd;

    public RexsterRecordReader(final RexsterConfiguration conf) {
        this.rexsterConf = conf;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        final RexsterInputSplit rexsterInputSplit = (RexsterInputSplit) inputSplit;
        this.splitEnd = rexsterInputSplit.getEnd();
        this.splitStart = rexsterInputSplit.getStart();

        if (this.rexsterConf.getMode().equals("rest")) {
            this.rexsterIterator = new RestIterator(new RexsterVertexLoaderImpl(this.rexsterConf),
                    rexsterInputSplit.getStart(), rexsterInputSplit.getEnd(), this.rexsterConf.getRexsterBuffer());
        } else if (this.rexsterConf.getMode().equals("kibble")) {
            this.rexsterIterator = new KibbleIterator(this.rexsterConf, rexsterInputSplit.getStart(),
                    rexsterInputSplit.getEnd());
        } else {
            // todo: catch this config error somewhere earlier would be better i think.
            throw new RuntimeException("???");
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean isNext = false;
        if (this.rexsterIterator.hasNext()) {
            final JSONObject nextJsonVertex = this.rexsterIterator.next();
            this.value = GraphSONUtility.fromJSON(nextJsonVertex.toString());
            isNext = true;
        }

        return isNext;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public FaunusVertex getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (this.splitStart == this.splitEnd) {
            return 0.0f;
        } else {
            // assuming you got the estimate right this progress should be pretty close;
            return Math.min(1.0f, this.rexsterIterator.getItemsIterated() / (float) this.rexsterConf.getEstimatedVertexCount());
        }
    }

    @Override
    public void close() throws IOException { }
}
