// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.core;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.*;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.builder.GraphDatabaseConfigurationBuilder;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.CfgManagementLoggerFactory;
import org.janusgraph.graphdb.management.JanusGraphManager;
import org.janusgraph.util.system.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;
import static org.janusgraph.graphdb.management.JanusGraphManager.JANUS_GRAPH_MANAGER_EXPECTED_STATE_MSG;

/**
 * JanusGraphFactory is used to open or instantiate a JanusGraph graph database.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see JanusGraph
 */

public class CfgJanusGraphFactory extends  JanusGraphFactory {

    private static final Logger log =
            LoggerFactory.getLogger(CfgJanusGraphFactory.class);
    /**
     * Opens a {@link JanusGraph} database configured according to the provided configuration.
     * This method shouldn't be called by end users; it is used by internal server processes to
     * open graphs defined at server start that do not include the graphname property.
     *
     * @param configuration Configuration for the graph database
     * @param backupName Backup name for graph
     * @return JanusGraph graph database
     */
    public static JanusGraph open(ReadConfiguration configuration, String backupName) {
        final ModifiableConfiguration config = new ModifiableConfiguration(ROOT_NS, (WriteConfiguration) configuration, BasicConfiguration.Restriction.NONE);
        final String graphName = config.has(GRAPH_NAME) ? config.get(GRAPH_NAME) : backupName;
        final JanusGraphManager jgm = JanusGraphManagerUtility.getInstance();
        if (null != graphName) {
            Preconditions.checkNotNull(jgm, JANUS_GRAPH_MANAGER_EXPECTED_STATE_MSG);
            return (JanusGraph) jgm.openGraph(graphName, gName -> new StandardJanusGraph(new GraphDatabaseConfigurationBuilder().build(configuration), new CfgManagementLoggerFactory()));
        } else {
            if (jgm != null) {
                log.warn("You should supply \"graph.graphname\" in your .properties file configuration if you are opening " +
                         "a graph that has not already been opened at server start, i.e. it was " +
                         "defined in your YAML file. This will ensure the graph is tracked by the JanusGraphManager, " +
                         "which will enable autocommit and rollback functionality upon all gremlin script executions. " +
                         "Note that JanusGraphFactory#open(String === shortcut notation) does not support consuming the property " +
                         "\"graph.graphname\" so these graphs should be accessed dynamically by supplying a .properties file here " +
                         "or by using the ConfiguredGraphFactory.");
            }
            if (jgm == null) {
                return new StandardJanusGraph(new GraphDatabaseConfigurationBuilder().build(configuration), new CfgManagementLoggerFactory());
            }
            return new StandardJanusGraph(new GraphDatabaseConfigurationBuilder().build(configuration));
        }
    }

    /**
     *  Return a Set of graph names stored in the {@link JanusGraphManager}
     *
     *  @return Set&lt;String&gt;
     */
    public static Set<String> getGraphNames() {
       final JanusGraphManager jgm = JanusGraphManagerUtility.getInstance();
       Preconditions.checkNotNull(jgm, JANUS_GRAPH_MANAGER_EXPECTED_STATE_MSG);
       return jgm.getGraphNames();
    }

    /**
     * Removes {@link Graph} from {@link JanusGraphManager} graph reference tracker, if exists
     * there.
     *
     * @param graph Graph
     */
    public static void close(Graph graph) throws Exception {
        final JanusGraphManager jgm = JanusGraphManagerUtility.getInstance();
        if (jgm != null) {
            jgm.removeGraph(((StandardJanusGraph) graph).getGraphName());
        }
        graph.close();
    }

    /**
     * Drop graph database, deleting all data in storage and indexing backends. Graph can be open or closed (will be
     * closed as part of the drop operation). The graph is also removed from the {@link JanusGraphManager}
     * graph reference tracker, if there.
     *
     * <p><b>WARNING: This is an irreversible operation that will delete all graph and index data.</b></p>
     * @param graph JanusGraph graph database. Can be open or closed.
     * @throws BackendException If an error occurs during deletion
     */
    public static void drop(JanusGraph graph) throws BackendException {
        Preconditions.checkNotNull(graph);
        Preconditions.checkArgument(graph instanceof StandardJanusGraph,"Invalid graph instance detected: %s",graph.getClass());
        final StandardJanusGraph g = (StandardJanusGraph) graph;
        final JanusGraphManager jgm = JanusGraphManagerUtility.getInstance();
        if (jgm != null) {
            jgm.removeGraph(g.getGraphName());
        }
        if (graph.isOpen()) {
            graph.close();
        }
        final GraphDatabaseConfiguration config = g.getConfiguration();
        final Backend backend = config.getBackend();
        try {
            backend.clearStorage();
        } finally {
            IOUtils.closeQuietly(backend);
        }
    }
}
