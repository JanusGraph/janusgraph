package com.thinkaurelius.titan.hadoop.formats.hbase;

import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.FaunusVertexQueryFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanHBaseRecordReader extends RecordReader<NullWritable, FaunusVertex> {

    private TableRecordReader reader;
    private TitanHBaseHadoopGraph graph;
    private FaunusVertexQueryFilter vertexQuery;
    private Configuration configuration;

    private FaunusVertex vertex;

    private final byte[] edgestoreFamilyBytes;

    public TitanHBaseRecordReader(final TitanHBaseHadoopGraph graph, final FaunusVertexQueryFilter vertexQuery, final TableRecordReader reader, final byte[] edgestoreFamilyBytes) {
        this.graph = graph;
        this.vertexQuery = vertexQuery;
        this.reader = reader;
        this.edgestoreFamilyBytes = edgestoreFamilyBytes;
    }

    @Override
    public void initialize(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.reader.initialize(inputSplit, taskAttemptContext);
        this.configuration = DEFAULT_COMPAT.getContextConfiguration(taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (this.reader.nextKeyValue()) {
            final FaunusVertex temp = this.graph.readHadoopVertex(this.configuration, this.reader.getCurrentKey().copyBytes(), this.reader.getCurrentValue().getMap().get(edgestoreFamilyBytes));
            if (null != temp) {
                this.vertex = temp;
                this.vertexQuery.filterRelationsOf(this.vertex);
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
        this.graph.close();
        this.reader.close();
    }

    @Override
    public float getProgress() {
        return this.reader.getProgress();
    }
}
