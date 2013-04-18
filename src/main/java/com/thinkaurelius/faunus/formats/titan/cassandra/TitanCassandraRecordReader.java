package com.thinkaurelius.faunus.formats.titan.cassandra;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.VertexQueryFilter;
import org.apache.cassandra.hadoop.ColumnFamilyRecordReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanCassandraRecordReader extends RecordReader<NullWritable, FaunusVertex> {

    private ColumnFamilyRecordReader reader;
    private FaunusTitanCassandraGraph graph;
    private boolean pathEnabled;
    private VertexQueryFilter vertexQuery;

    private FaunusVertex vertex;

    public TitanCassandraRecordReader(final FaunusTitanCassandraGraph graph, final VertexQueryFilter vertexQuery, final boolean pathEnabled, final ColumnFamilyRecordReader reader) {
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
            final FaunusVertex temp = this.graph.readFaunusVertex(this.reader.getCurrentKey().duplicate(), this.reader.getCurrentValue());
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
