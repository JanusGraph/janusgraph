package com.thinkaurelius.titan.hadoop.formats.cassandra;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import com.thinkaurelius.titan.diskstorage.configuration.Configuration;

import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphComputeVertex;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
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
public class TitanCassandraRecordReader extends RecordReader<NullWritable, GiraphComputeVertex> {

    private static final Logger log =
            LoggerFactory.getLogger(TitanCassandraRecordReader.class);

    private ColumnFamilyRecordReader reader;
    private TitanCassandraHadoopGraph graph;
    private Configuration configuration;
    private GiraphComputeVertex vertex;

    public TitanCassandraRecordReader(final TitanCassandraHadoopGraph graph, final ColumnFamilyRecordReader reader) {
        this.graph = graph;
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
            final TinkerVertex maybeNullTinkerVertex =
                    graph.readHadoopVertex(reader.getCurrentKey().duplicate(), reader.getCurrentValue());
            if (null != maybeNullTinkerVertex) {
                vertex = new GiraphComputeVertex(maybeNullTinkerVertex);
                //vertexQuery.filterRelationsOf(vertex); // TODO reimplement vertexquery filtering
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
    public GiraphComputeVertex getCurrentValue() throws IOException, InterruptedException {
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
