package com.thinkaurelius.faunus.formats.titan.hbase;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.VertexQueryFilter;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanHBaseRecordReader extends RecordReader<NullWritable, FaunusVertex> {

    private TableRecordReader reader;
    private FaunusTitanHBaseGraph graph;
    private VertexQueryFilter vertexQuery;
    private boolean pathEnabled;

    private FaunusVertex vertex;

    public TitanHBaseRecordReader(final FaunusTitanHBaseGraph graph, final VertexQueryFilter vertexQuery, final boolean pathEnabled, final TableRecordReader reader) {
        this.graph = graph;
        this.vertexQuery = vertexQuery;
        this.pathEnabled = pathEnabled;
        this.reader = reader;
    }

    @Override
    public void initialize(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.reader.initialize(inputSplit, taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (this.reader.nextKeyValue()) {
            final FaunusVertex temp = this.graph.readFaunusVertex(this.reader.getCurrentKey().copyBytes(), this.reader.getCurrentValue().getMap().get(TitanHBaseInputFormat.EDGE_STORE_FAMILY));
            if (null != temp) {
                if (this.pathEnabled) temp.enablePath(true);
                this.vertex = temp;
                this.vertexQuery.defaultFilter(this.vertex);
                return true;
            }
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public FaunusVertex getCurrentValue() throws IOException, InterruptedException {
        return this.vertex;
    }

    @Override
    public void close() throws IOException {
        this.graph.shutdown();
        this.reader.close();
    }

    @Override
    public float getProgress() {
        return this.reader.getProgress();
    }
}
