package com.thinkaurelius.titan.hadoop.formats.hbase;

import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.FaunusVertexQueryFilter;

import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
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
        reader.initialize(inputSplit, taskAttemptContext);
        configuration = ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getContextConfiguration(taskAttemptContext));
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (reader.nextKeyValue()) {
            final FaunusVertex temp = graph.readHadoopVertex(
                    configuration,
                    reader.getCurrentKey().copyBytes(),
                    reader.getCurrentValue().getMap().get(edgestoreFamilyBytes));
            if (null != temp) {
                vertex = temp;
                vertexQuery.filterRelationsOf(vertex);
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
