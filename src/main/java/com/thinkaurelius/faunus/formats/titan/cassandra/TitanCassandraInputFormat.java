package com.thinkaurelius.faunus.formats.titan.cassandra;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.titan.GraphFactory;
import com.thinkaurelius.faunus.formats.titan.TitanInputFormat;
import com.thinkaurelius.faunus.formats.titan.hbase.FaunusTitanHBaseGraph;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.titan.diskstorage.Backend;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ColumnFamilyRecordReader;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.commons.configuration.BaseConfiguration;
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

    public static final String TITAN_GRAPH_INPUT_STORAGE_KEYSPACE = "titan.graph.input.storage.keyspace";

    private final ColumnFamilyInputFormat columnFamilyInputFormat;
    private FaunusTitanCassandraGraph graph;
    private boolean pathEnabled;
    private Configuration config;


    public TitanCassandraInputFormat() {
        this.columnFamilyInputFormat = new ColumnFamilyInputFormat();
    }

    @Override
    public List<InputSplit> getSplits(final JobContext jobContext) throws IOException, InterruptedException {
        return this.columnFamilyInputFormat.getSplits(jobContext);
    }

    @Override
    public RecordReader<NullWritable, FaunusVertex> createRecordReader(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new TitanCassandraRecordReader(this.graph, this.pathEnabled, (ColumnFamilyRecordReader) this.columnFamilyInputFormat.createRecordReader(inputSplit, taskAttemptContext));
    }

    @Override
    public void setConf(final Configuration config) {
        config.set("cassandra.input.keyspace", config.get(TITAN_GRAPH_INPUT_STORAGE_KEYSPACE));
        ConfigHelper.setInputColumnFamily(config, ConfigHelper.getInputKeyspace(config), Backend.EDGESTORE_NAME);
        final SlicePredicate predicate = new SlicePredicate();
        final SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(new byte[0]);
        sliceRange.setFinish(new byte[0]);
        sliceRange.setCount(Integer.MAX_VALUE);
        predicate.setSlice_range(sliceRange);
        ConfigHelper.setInputSlicePredicate(config, predicate);

        ConfigHelper.setInputInitialAddress(config, config.get(TITAN_GRAPH_INPUT_STORAGE_HOSTNAME));
        ConfigHelper.setInputRpcPort(config, config.get(TITAN_GRAPH_INPUT_STORAGE_PORT));
        config.set("storage.read-only","true");
        config.set("autotype","none");
        this.graph = new FaunusTitanCassandraGraph(GraphFactory.generateTitanConfiguration(config, TITAN_GRAPH_INPUT));
        this.pathEnabled = config.getBoolean(FaunusCompiler.PATH_ENABLED, false);

        this.pathEnabled = config.getBoolean(FaunusCompiler.PATH_ENABLED, false);
        this.config = config;
    }

    @Override
    public Configuration getConf() {
        return this.config;
    }


}
