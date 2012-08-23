package com.thinkaurelius.faunus.formats.rexster;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.graphson.GraphSONUtility;
import com.tinkerpop.blueprints.Direction;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.hsqldb.lib.StringInputStream;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RexsterRecordReader extends RecordReader<NullWritable, FaunusVertex> {

    private final RexsterConfiguration rexsterConf;
    private RexsterIterator rexsterIterator;

    private final NullWritable key = NullWritable.get();
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
        this.rexsterIterator = new RexsterIterator(new RexsterVertexLoaderImpl(this.rexsterConf),
                rexsterInputSplit.getStart(), rexsterInputSplit.getEnd(), 256);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean isNext = false;
        if (this.rexsterIterator.hasNext()) {
            final JSONObject nextJsonVertex = this.rexsterIterator.next();
            this.value = GraphSONUtility.fromJSON(nextJsonVertex.toString());

            /*
                    new FaunusVertex(nextJsonVertex.optLong("_id"));

            final JSONArray outEdgesJsonArray = nextJsonVertex.optJSONArray("_outEdges");
            for (int ix = 0; ix < outEdgesJsonArray.length(); ix++) {
                final JSONObject edgeJson = outEdgesJsonArray.optJSONObject(ix);
                final FaunusEdge e = new FaunusEdge(edgeJson.optLong("_outV"),
                        edgeJson.optLong("_inV"), edgeJson.optString("_label"));
                this.value.addEdge(Direction.OUT, e);
            }

            final JSONArray inEdgesJsonArray = nextJsonVertex.optJSONArray("_inEdges");
            for (int ix = 0; ix < inEdgesJsonArray.length(); ix++) {
                final JSONObject edgeJson = inEdgesJsonArray.optJSONObject(ix);
                final FaunusEdge e = new FaunusEdge(edgeJson.optLong("_outV"),
                        edgeJson.optLong("_inV"), edgeJson.optString("_label"));
                this.value.addEdge(Direction.IN, e);
            }
            */

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
