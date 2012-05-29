package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.graphdb.blueprints.TitanInMemoryBlueprintsGraph;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.transaction.InMemoryTitanGraph;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfig;
import org.apache.commons.configuration.Configuration;

/**
 * TitanFactory is used to open or instantiate a Titan graph database.
 *
 * @see TitanGraph
 * @author	Matthias Br&ouml;cheler (http://www.matthiasb.com)
 */

public class TitanFactory {


    /**
     * Opens and returns an in-memory Titan graph database.
     *
     * An in-memory graph database does not persist any data to disk but keeps it entirely in memory.
     * Note that in-memory graph databases do not support multiple transactions. If multiple in-memory
     * transactions are needed, open multiple in-memory databases.
     *
     * @return An in-memory graph database
     * @see TitanGraph
     */
    public static TitanGraph openInMemoryGraph() {
        return new TitanInMemoryBlueprintsGraph();
    }

    /**
     * Opens a {@link TitanGraph} database.
     *
     * If the argument points to a configuration file, the configuration file is loaded to configure the database.
     * If the argument points to a path, a graph database is created or opened at that location.
     * 
     * @param directoryOrConfigFile Configuration file name or directory name
     * @return Titan graph database
     * @see <a href="https://github.com/thinkaurelius/titan/wiki/Graph-Configuration">Graph Configuration Wiki</a>
     *
     */
    public static TitanGraph open(String directoryOrConfigFile) {
        GraphDatabaseConfiguration config = new GraphDatabaseConfiguration(directoryOrConfigFile);
        return new StandardTitanGraph(config);
    }

    /**
     * Opens a {@link TitanGraph} database configured according to the provided configuration.
     *
     * @param configuration Configuration for the graph database
     * @return Titan graph database
     * @see <a href="https://github.com/thinkaurelius/titan/wiki/Graph-Configuration">Graph Configuration Wiki</a>
     *
     */
    public static TitanGraph open(Configuration configuration) {
        GraphDatabaseConfiguration config = new GraphDatabaseConfiguration(configuration);
        return new StandardTitanGraph(config);
    }
    
}
