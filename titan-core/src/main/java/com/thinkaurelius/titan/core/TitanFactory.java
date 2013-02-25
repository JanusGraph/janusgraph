package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import org.apache.commons.configuration.Configuration;

import java.io.File;

/**
 * TitanFactory is used to open or instantiate a Titan graph database.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see TitanGraph
 */

public class TitanFactory {

    /**
     * Opens a {@link TitanGraph} database.
     * <p/>
     * If the argument points to a configuration file, the configuration file is loaded to configure the database.
     * If the argument points to a path, a graph database is created or opened at that location.
     *
     * @param directoryOrConfigFile Configuration file name or directory name
     * @return Titan graph database
     * @see <a href="https://github.com/thinkaurelius/titan/wiki/Graph-Configuration">Graph Configuration Wiki</a>
     */
    public static TitanGraph open(String directoryOrConfigFile) {
        return open(GraphDatabaseConfiguration.getConfiguration(new File(directoryOrConfigFile)));
    }

    /**
     * Opens a {@link TitanGraph} database configured according to the provided configuration.
     *
     * @param configuration Configuration for the graph database
     * @return Titan graph database
     * @see <a href="https://github.com/thinkaurelius/titan/wiki/Graph-Configuration">Graph Configuration Wiki</a>
     */
    public static TitanGraph open(Configuration configuration) {
        return new StandardTitanGraph(new GraphDatabaseConfiguration(configuration));
    }

}
