package com.thinkaurelius.faunus.formats.titan.cassandra;

import com.thinkaurelius.faunus.formats.titan.TitanOutputFormat;
import com.thinkaurelius.faunus.formats.titan.TitanRecordWriter;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanCassandraOutputFormat extends TitanOutputFormat {
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

        this.graph = new FaunusTitanCassandraGraph(titanconfig, false);
        this.graph.createKeyIndex(TitanRecordWriter.FAUNUS_IDX_ID, Vertex.class);
        this.graph.stopTransaction(TransactionalGraph.Conclusion.SUCCESS);

        this.config = config;
    }
}