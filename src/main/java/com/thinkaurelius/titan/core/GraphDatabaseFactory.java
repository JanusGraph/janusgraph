package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardGraphDB;
import com.thinkaurelius.titan.graphdb.transaction.InMemoryGraphDB;
import org.apache.commons.configuration.Configuration;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class GraphDatabaseFactory {


    /**
     * Opens and returns an in-memory graph database.
     *
     * An in-memory graph database does not persist any data to disk but keeps it entirely in memory.
     * Note that in-memory graph databases do not support multiple transactions. If multiple in-memory
     * transactions are needed, open multiple in-memory databases.
     *
     * @return An in-memory graph database
     * @see GraphDatabase
     */
    public static GraphDatabase openInMemoryGraph() {
        return new InMemoryGraphDB();
    }


    public static GraphDatabase open(String directoryOrConfigFile) {
        GraphDatabaseConfiguration config = new GraphDatabaseConfiguration(directoryOrConfigFile);
        return new StandardGraphDB(config);
    }
    
    public static GraphDatabase open(Configuration configuration) {
        GraphDatabaseConfiguration config = new GraphDatabaseConfiguration(configuration);
        return new StandardGraphDB(config);
    }
    
}
