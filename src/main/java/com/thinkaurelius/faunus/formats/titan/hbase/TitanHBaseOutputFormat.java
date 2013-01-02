package com.thinkaurelius.faunus.formats.titan.hbase;

import com.thinkaurelius.faunus.formats.titan.TitanOutputFormat;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanHBaseOutputFormat extends TitanOutputFormat {

    private static final String HOSTNAME_KEY = HBaseStoreManager.HBASE_CONFIGURATION_MAP.get(GraphDatabaseConfiguration.HOSTNAME_KEY);
    private static final String PORT_KEY = HBaseStoreManager.HBASE_CONFIGURATION_MAP.get(GraphDatabaseConfiguration.PORT_KEY);

    @Override
    public void setConf(final Configuration config) {
        config.set(TableOutputFormat.OUTPUT_TABLE, config.get(TableOutputFormat.OUTPUT_TABLE));

        final BaseConfiguration titanconfig = new BaseConfiguration();
        //General Titan configuration for read-only
        //titanconfig.setProperty("storage.read-only", "true"); // TODO: Should we make this simply false?
        titanconfig.setProperty("autotype", "blueprints");
        // HBase specific configuration
        titanconfig.setProperty("storage.backend", "hbase");
        titanconfig.setProperty("storage.tablename", config.get(TableOutputFormat.OUTPUT_TABLE));
        titanconfig.setProperty("storage.hostname", config.get(HOSTNAME_KEY));
        if (config.get(PORT_KEY, null) != null)
            titanconfig.setProperty("storage.port", config.get(PORT_KEY));
        this.graph = new FaunusTitanHBaseGraph(titanconfig);
        //this.pathEnabled = config.getBoolean(FaunusCompiler.PATH_ENABLED, false);
    }
}