package com.thinkaurelius.titan.hadoop.formats.cassandra;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.FaunusVertexQueryFilter;

import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import org.apache.cassandra.hadoop.ColumnFamilyRecordReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanCassandraRecordReader extends RecordReader<NullWritable, FaunusVertex> {

    private static final Logger log =
            LoggerFactory.getLogger(TitanCassandraRecordReader.class);

    private ColumnFamilyRecordReader reader;
    private TitanCassandraHadoopGraph graph;
    private FaunusVertexQueryFilter vertexQuery;
    private Configuration configuration;
    private FaunusVertex vertex;

    public TitanCassandraRecordReader(final TitanCassandraHadoopGraph graph, final FaunusVertexQueryFilter vertexQuery, final ColumnFamilyRecordReader reader) {
        this.graph = graph;
        this.vertexQuery = vertexQuery;
        this.reader = reader;
    }

    @Override
    public void initialize(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        reader.initialize(inputSplit, taskAttemptContext);
        configuration = ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getContextConfiguration(taskAttemptContext));
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (reader.nextKeyValue()) {
            // TODO titan05 integration -- the duplicate() call may be unnecessary
            final FaunusVertex temp = graph.readHadoopVertex(configuration, reader.getCurrentKey().duplicate(), reader.getCurrentValue());
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
        return vertex;
    }

    @Override
    public void close() throws IOException {
        graph.close();
        reader.close();
    }

    @Override
    public float getProgress() {
        return reader.getProgress();
    }
}
