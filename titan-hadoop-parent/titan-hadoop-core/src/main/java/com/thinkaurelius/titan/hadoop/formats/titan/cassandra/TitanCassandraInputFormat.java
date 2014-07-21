package com.thinkaurelius.titan.hadoop.formats.titan.cassandra;

import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.titan.TitanInputFormat;

import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ColumnFamilyRecordReader;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanCassandraInputFormat extends TitanInputFormat {

//    public static final String TITAN_HADOOP_GRAPH_INPUT_TITAN_STORAGE_KEYSPACE = "titan.hadoop.input.storage.keyspace";

    private final ColumnFamilyInputFormat columnFamilyInputFormat = new ColumnFamilyInputFormat();
    private TitanCassandraHadoopGraph graph;
    private Configuration config;

    @Override
    public List<InputSplit> getSplits(final JobContext jobContext) throws IOException, InterruptedException {
        return this.columnFamilyInputFormat.getSplits(jobContext);
    }

    @Override
    public RecordReader<NullWritable, FaunusVertex> createRecordReader(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new TitanCassandraRecordReader(this.graph, this.vertexQuery, (ColumnFamilyRecordReader) this.columnFamilyInputFormat.createRecordReader(inputSplit, taskAttemptContext));
    }

    @Override
    public void setConf(final Configuration config) {
        super.setConf(config);
        this.titanInputConf = TitanHadoopConfiguration.of(config).extractInputGraphConfiguration();
        this.graph = new TitanCassandraHadoopGraph(titanSetup);

        config.set("cassandra.input.keyspace", titanInputConf.get(AbstractCassandraStoreManager.CASSANDRA_KEYSPACE));
        ConfigHelper.setInputColumnFamily(config, ConfigHelper.getInputKeyspace(config), Backend.EDGESTORE_NAME);
        final SlicePredicate predicate = new SlicePredicate();
        predicate.setSlice_range(getSliceRange(titanSetup.inputSlice(vertexQuery), config.getInt("cassandra.range.batch.size", Integer.MAX_VALUE)));
        ConfigHelper.setInputSlicePredicate(config, predicate);
        ConfigHelper.setInputInitialAddress(config, titanInputConf.get(GraphDatabaseConfiguration.STORAGE_HOSTS)[0]);
        if (titanInputConf.has(GraphDatabaseConfiguration.STORAGE_PORT))
            ConfigHelper.setInputRpcPort(config, String.valueOf(titanInputConf.get(GraphDatabaseConfiguration.STORAGE_PORT)));
        if (titanInputConf.has(GraphDatabaseConfiguration.AUTH_USERNAME))
            ConfigHelper.setInputKeyspaceUserName(config, titanInputConf.get(GraphDatabaseConfiguration.AUTH_USERNAME));
        if (titanInputConf.has(GraphDatabaseConfiguration.AUTH_PASSWORD))
            ConfigHelper.setInputKeyspacePassword(config, titanInputConf.get(GraphDatabaseConfiguration.AUTH_PASSWORD));
        // TODO config.set("storage.read-only", "true");
        config.set("autotype", "none");

        this.config = config;
    }

    private SliceRange getSliceRange(final SliceQuery slice, final int limit) {
        final SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(slice.getSliceStart().asByteBuffer());
        sliceRange.setFinish(slice.getSliceEnd().asByteBuffer());
        sliceRange.setCount(Math.min(limit, slice.getLimit()));
        return sliceRange;
    }

    @Override
    public Configuration getConf() {
        return this.config;
    }
}
