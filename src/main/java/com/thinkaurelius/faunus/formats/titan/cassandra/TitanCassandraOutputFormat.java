package com.thinkaurelius.faunus.formats.titan.cassandra;

import com.thinkaurelius.faunus.formats.titan.TitanOutputFormat;
import com.thinkaurelius.titan.core.TitanFactory;
import com.tinkerpop.blueprints.Graph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanCassandraOutputFormat extends TitanOutputFormat {

    public static final String TITAN_GRAPH_OUTPUT_STORAGE_KEYSPACE = "titan.graph.output.storage.keyspace";

    public static Graph generateGraph(final Configuration config) {
        final BaseConfiguration titanconfig = new BaseConfiguration();
        titanconfig.setProperty("autotype", "blueprints");
        titanconfig.setProperty("storage.backend", config.get(TITAN_GRAPH_OUTPUT_STORAGE_BACKEND));
        titanconfig.setProperty("storage.hostname", config.get(TITAN_GRAPH_OUTPUT_STORAGE_HOSTNAME));
        titanconfig.setProperty("storage.keyspace", config.get(TITAN_GRAPH_OUTPUT_STORAGE_KEYSPACE));
        titanconfig.setProperty("storage.port", config.get(TITAN_GRAPH_OUTPUT_STORAGE_PORT));
        titanconfig.setProperty("storage.batch-loading", true);
        return TitanFactory.open(titanconfig);
    }
}