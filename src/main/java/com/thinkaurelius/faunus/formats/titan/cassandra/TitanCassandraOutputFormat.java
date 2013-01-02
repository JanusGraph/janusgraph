package com.thinkaurelius.faunus.formats.titan.cassandra;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.titan.diskstorage.Backend;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanCassandraOutputFormat extends OutputFormat<NullWritable, FaunusVertex> implements Configurable {

    private final ColumnFamilyOutputFormat columnFamilyOutputFormat;
    private FaunusTitanCassandraGraph graph;
    private Configuration config;


    public TitanCassandraOutputFormat() {
        this.columnFamilyOutputFormat = new ColumnFamilyOutputFormat();
    }

    @Override
    public void checkOutputSpecs(final JobContext context) throws InterruptedException, IOException {
        this.columnFamilyOutputFormat.checkOutputSpecs(context);
    }

    @Override
    public OutputCommitter getOutputCommitter(final TaskAttemptContext context) throws InterruptedException, IOException {
        return this.columnFamilyOutputFormat.getOutputCommitter(context);
    }

    @Override
    public RecordWriter<NullWritable, FaunusVertex> getRecordWriter(final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new TitanCassandraRecordWriter(this.graph, this.columnFamilyOutputFormat.getRecordWriter(taskAttemptContext));
    }

    @Override
    public void setConf(final Configuration config) {
        ConfigHelper.setOutputColumnFamily(config, ConfigHelper.getOutputKeyspace(config), Backend.EDGESTORE_NAME);

        final BaseConfiguration titanconfig = new BaseConfiguration();
        //General Titan configuration for read-only
        //titanconfig.setProperty("storage.read-only", "true"); // TODO: Should we make this simply false?
        titanconfig.setProperty("autotype", "blueprints");
        //Cassandra specific configuration
        titanconfig.setProperty("storage.backend", "cassandra");   // todo: astyanax
        titanconfig.setProperty("storage.hostname", ConfigHelper.getOutputInitialAddress(config));
        titanconfig.setProperty("storage.keyspace", ConfigHelper.getOutputKeyspace(config));
        titanconfig.setProperty("storage.port", ConfigHelper.getOutputRpcPort(config));
        if (ConfigHelper.getReadConsistencyLevel(config) != null)
            titanconfig.setProperty("storage.read-consistency-level", ConfigHelper.getReadConsistencyLevel(config));
        if (ConfigHelper.getWriteConsistencyLevel(config) != null)
            titanconfig.setProperty("storage.write-consistency-level", ConfigHelper.getWriteConsistencyLevel(config));

        this.graph = new FaunusTitanCassandraGraph(titanconfig, false);
        //this.pathEnabled = config.getBoolean(FaunusCompiler.PATH_ENABLED, false);
        this.config = config;
    }

    @Override
    public Configuration getConf() {
        return this.config;
    }

}