package com.thinkaurelius.faunus.formats.sequence.faunus01;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.sequence.faunus01.util.FaunusEdge01;
import com.thinkaurelius.faunus.formats.sequence.faunus01.util.FaunusVertex01;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.util.ElementHelper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusSequenceFileRecordReader extends RecordReader<NullWritable, FaunusVertex> {

    private final SequenceFileRecordReader<NullWritable, FaunusVertex01> recordReader = new SequenceFileRecordReader<NullWritable, FaunusVertex01>();

    @Override
    public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        this.recordReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return this.recordReader.nextKeyValue();
    }

    @Override
    public NullWritable getCurrentKey() {
        return this.recordReader.getCurrentKey();
    }

    @Override
    public FaunusVertex getCurrentValue() {
        return this.buildFaunusVertex(this.recordReader.getCurrentValue());
    }

    @Override
    public float getProgress() throws IOException {
        return this.recordReader.getProgress();
    }

    @Override
    public synchronized void close() throws IOException {
        this.recordReader.close();
    }

    private FaunusVertex buildFaunusVertex(FaunusVertex01 vertex01) {

        final FaunusVertex vertex = new FaunusVertex(vertex01.getIdAsLong());
        ElementHelper.copyProperties(vertex01, vertex);
        for (final Edge temp : vertex01.getEdges(Direction.OUT)) {
            final FaunusEdge01 edge01 = (FaunusEdge01) temp;
            final FaunusEdge edge = new FaunusEdge(vertex.getIdAsLong(), edge01.getVertexId(Direction.IN), edge01.getLabel());
            ElementHelper.copyProperties(edge01, vertex01);
            vertex.addEdge(Direction.OUT, edge);
        }
        for (final Edge temp : vertex01.getEdges(Direction.IN)) {
            final FaunusEdge01 edge01 = (FaunusEdge01) temp;
            final FaunusEdge edge = new FaunusEdge(edge01.getVertexId(Direction.IN), vertex.getIdAsLong(), edge01.getLabel());
            ElementHelper.copyProperties(edge01, vertex01);
            vertex.addEdge(Direction.IN, edge);
        }
        return vertex;
    }
}
