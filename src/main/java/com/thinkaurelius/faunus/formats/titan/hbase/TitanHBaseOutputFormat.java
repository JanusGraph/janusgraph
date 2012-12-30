package com.thinkaurelius.faunus.formats.titan.hbase;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanHBaseOutputFormat extends OutputFormat implements Configurable {

    private static final String HOSTNAME_KEY = HBaseStoreManager.HBASE_CONFIGURATION_MAP.get(GraphDatabaseConfiguration.HOSTNAME_KEY);
    private static final String PORT_KEY = HBaseStoreManager.HBASE_CONFIGURATION_MAP.get(GraphDatabaseConfiguration.PORT_KEY);
    static final byte[] EDGE_STORE_FAMILY = Bytes.toBytes(Backend.EDGESTORE_NAME);

    private final TableOutputFormat tableOutputFormat;
    private FaunusTitanHBaseGraph graph;


    public TitanHBaseOutputFormat() {
        this.tableOutputFormat = new TableOutputFormat();
    }

    @Override
    public void checkOutputSpecs(final JobContext context) throws InterruptedException, IOException {
        this.tableOutputFormat.checkOutputSpecs(context);
    }

    @Override
    public OutputCommitter getOutputCommitter(final TaskAttemptContext context) throws InterruptedException, IOException {
        return this.tableOutputFormat.getOutputCommitter(context);
    }

    @Override
    public RecordWriter<NullWritable, FaunusVertex> getRecordWriter(final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new TitanHBaseRecordWriter(this.graph, this.tableOutputFormat.getRecordWriter(taskAttemptContext));
    }


    @Override
    public void setConf(final Configuration config) {

        config.set(TableOutputFormat.OUTPUT_TABLE, config.get(TableOutputFormat.OUTPUT_TABLE));
        this.tableOutputFormat.setConf(config);

        final BaseConfiguration titanconfig = new BaseConfiguration();
        //General Titan configuration for read-only
        //titanconfig.setProperty("storage.read-only", "true"); // TODO: Should we make this simply false?
        titanconfig.setProperty("autotype", "none");
        // HBase specific configuration
        titanconfig.setProperty("storage.backend", "hbase");
        titanconfig.setProperty("storage.tablename", config.get(TableOutputFormat.OUTPUT_TABLE));
        titanconfig.setProperty("storage.hostname", config.get(HOSTNAME_KEY));
        if (config.get(PORT_KEY, null) != null)
            titanconfig.setProperty("storage.port", config.get(PORT_KEY));
        this.graph = new FaunusTitanHBaseGraph(titanconfig);
        //this.pathEnabled = config.getBoolean(FaunusCompiler.PATH_ENABLED, false);
    }

    @Override
    public Configuration getConf() {
        return tableOutputFormat.getConf();
    }
}