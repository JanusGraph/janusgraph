package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.transaction.InMemoryTitanGraph;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfig;
import org.apache.commons.configuration.Configuration;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class TitanFactory {


    /**
     * Opens and returns an in-memory graph database.
     *
     * An in-memory graph database does not persist any data to disk but keeps it entirely in memory.
     * Note that in-memory graph databases do not support multiple transactions. If multiple in-memory
     * transactions are needed, open multiple in-memory databases.
     *
     * @return An in-memory graph database
     * @see TitanGraph
     */
    public static TitanGraph openInMemoryGraph() {
        return new InMemoryTitanGraph(new TransactionConfig(null,true));
    }


    public static TitanGraph open(String directoryOrConfigFile) {
        GraphDatabaseConfiguration config = new GraphDatabaseConfiguration(directoryOrConfigFile);
        return new StandardTitanGraph(config);
    }
    
    public static TitanGraph open(Configuration configuration) {
        GraphDatabaseConfiguration config = new GraphDatabaseConfiguration(configuration);
        return new StandardTitanGraph(config);
    }
    
}
