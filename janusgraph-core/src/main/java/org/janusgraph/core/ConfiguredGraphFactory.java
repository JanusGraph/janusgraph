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

import org.janusgraph.graphdb.management.ConfigurationManagementGraph;
import org.janusgraph.graphdb.management.JanusGraphManager;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.management.utils.ConfigurationManagementGraphNotEnabledException;
import static org.janusgraph.graphdb.management.JanusGraphManager.JANUS_GRAPH_MANAGER_EXPECTED_STATE_MSG;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;

import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.Objects;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides static methods to: 1) create graph references denoted by a
 * graphName using a previously created template configuration using the
 * {@link ConfigurationManagementGraph} or 2) open a graph reference denoted by a
 * graphName for graphs that have been previously created or for graphs for which you have
 * previously created a configuration using the {@link ConfigurationManagementGraph}. This
 * class also defines a close which allows for removal of these {@link Graph} objects from the
 * {@link JanusGraphManager} reference tracker and closes the graph as well a drop which completely
 * clears all associated graph and index data as well as removes the graph configuration from the
 * {@link ConfigurationManagementGraph}.
 * <p>This class allows you to create/update/remove configuration objects used to open a graph.
 * To use these APIs, you must define one of your graph's key as "ConfigurationManagementGraph"
 * in your server's YAML; the configuration objects you define using these APIs will be stored
 * in a graph representation (on a vertex, more precisely), and this graph will be opened
 * according to the file supplied along with the "ConfigurationManagementGraph" key.</p>
 */
public class ConfiguredGraphFactory {

    private static final Logger log =
    LoggerFactory.getLogger(ConfiguredGraphFactory.class);

    /**
     * Creates a {@link JanusGraph} configuration stored in the {@link ConfigurationManagementGraph}
     * graph and a {@link JanusGraph} graph reference according to the single
     * Template Configuration  previously created by the {@link ConfigurationManagementGraph} API;
     * A configuration for this graph must not already exist, and a Template Configuration must
     * exist. If the Template Configuration does not include its
     * backend's respective keyspace/table/storage_directory parameter, we set the keyspace/table
     * to the supplied graphName or we append the graphName to the supplied
     * storage_root parameter.
     *
     * @param graphName
     *
     * @return JanusGraph
     */
    public static synchronized JanusGraph create(final String graphName) {
        final ConfigurationManagementGraph configManagementGraph = getConfigGraphManagementInstance();

        final Map<String, Object> graphConfigMap = configManagementGraph.getConfiguration(graphName);
        Preconditions.checkState(null == graphConfigMap, String.format("Configuration for graph %s already exists.", graphName));
        final Map<String, Object> templateConfigMap = configManagementGraph.getTemplateConfiguration();
        Preconditions.checkState(null != templateConfigMap,
                                "Please create a template Configuration using the ConfigurationManagementGraph#createTemplateConfiguration API.");
        templateConfigMap.put(ConfigurationManagementGraph.PROPERTY_GRAPH_NAME, graphName);
        templateConfigMap.put(ConfigurationManagementGraph.PROPERTY_CREATED_USING_TEMPLATE, true);

        final JanusGraphManager jgm = JanusGraphManagerUtility.getInstance();
        Preconditions.checkState(jgm != null, JANUS_GRAPH_MANAGER_EXPECTED_STATE_MSG);
        final CommonsConfiguration config = new CommonsConfiguration(new MapConfiguration(templateConfigMap));
        final JanusGraph g = (JanusGraph) jgm.openGraph(graphName, (String gName) -> new StandardJanusGraph(new GraphDatabaseConfiguration(config)));
        configManagementGraph.createConfiguration(new MapConfiguration(templateConfigMap));
        return g;
    }

    /**
     * Open a {@link JanusGraph} using a previously created Configuration using the
     * {@link ConfigurationManagementGraph} API. A corresponding configuration must exist.
     *
     * <p>NOTE: If your configuration corresponding to this graph does not contain information about
     * the backend's keyspace/table/storage directory, then we set the keyspace/table to the
     * graphName or set the storage directory to the storage_root + /graphName.</p>
     *
     * @param graphName
     *
     * @return JanusGraph
     */
    public static JanusGraph open(String graphName) {
        final ConfigurationManagementGraph configManagementGraph = getConfigGraphManagementInstance();
        final Map<String, Object> graphConfigMap = configManagementGraph.getConfiguration(graphName);
        Preconditions.checkState(null != graphConfigMap,
                                "Please create configuration for this graph using the ConfigurationManagementGraph#createConfiguration API.");
        final JanusGraphManager jgm = JanusGraphManagerUtility.getInstance();
        Preconditions.checkState(jgm != null, JANUS_GRAPH_MANAGER_EXPECTED_STATE_MSG);
        final CommonsConfiguration config = new CommonsConfiguration(new MapConfiguration(graphConfigMap));
        return (JanusGraph) jgm.openGraph(graphName, (String gName) -> new StandardJanusGraph(new GraphDatabaseConfiguration(config)));
    }

    /**
     * Get a Set of the graphNames that exist in your configuration management graph
     *
     */
    public static Set<String> getGraphNames() {
        final ConfigurationManagementGraph configManagementGraph = getConfigGraphManagementInstance();
        final List<Map<String, Object>> configurations = configManagementGraph.getConfigurations();
        return configurations.stream()
            .map(elem -> (String) elem.getOrDefault(ConfigurationManagementGraph.PROPERTY_GRAPH_NAME, null))
            .filter(Objects::nonNull).collect(Collectors.toSet());
    }

    /**
     * Removes the graph corresponding to the supplied graphName
     * from the {@link JanusGraphManager} graph reference tracker and
     * returns the corresponding Graph, or null if it doesn't exist.
     *
     * @param graphName Graph
     * @return JanusGraph
     */
    public static JanusGraph close(String graphName) throws Exception {
        final JanusGraphManager jgm = JanusGraphManagerUtility.getInstance();
        Preconditions.checkState(jgm != null, JANUS_GRAPH_MANAGER_EXPECTED_STATE_MSG);
        final Graph graph = jgm.removeGraph(graphName);
        if (null != graph) graph.close();
        return (JanusGraph) graph;
    }

    /**
     * Drop graph database, deleting all data in storage and indexing backends. Graph can be open or closed (will be
     * closed as part of the drop operation). The graph is removed from the {@link
     * JanusGraphManager} graph reference tracker, if there. Finally, if a configuration for this
     * graph exists on the {@link ConfigurationManagementGraph}, then said configuration will be
     * removed.
     *
     * <p><b>WARNING: This is an irreversible operation that will delete all graph and index data.</b></p>
     * @param graphName String graphName. Corresponding graph can be open or closed.
     * @throws BackendException If an error occurs during deletion
     * @throws ConfigurationManagementGraphNotEnabledException If ConfigurationManagementGraph not
     */
    public static void drop(String graphName) throws BackendException, ConfigurationManagementGraphNotEnabledException, Exception {
        final StandardJanusGraph graph = (StandardJanusGraph) ConfiguredGraphFactory.close(graphName);
        JanusGraphFactory.drop(graph);
        removeConfiguration(graphName);
    }

    private static ConfigurationManagementGraph getConfigGraphManagementInstance() {
        final ConfigurationManagementGraph configManagementGraph;
        try {
            configManagementGraph = ConfigurationManagementGraph.getInstance();
        } catch (ConfigurationManagementGraphNotEnabledException e) {
            throw new RuntimeException(e);
        }
        return configManagementGraph;
    }

    /**
     * Create a configuration according to the supplied {@link Configuration}; you must include
     * the property "graph.graphname" with a value in the configuration; you can then
     * open your graph using graph.graphname without having to supply the
     * Configuration or File each time using the {@link org.janusgraph.core.ConfiguredGraphFactory}.
     */
    public static void createConfiguration(final Configuration config) {
        final ConfigurationManagementGraph configManagementGraph = getConfigGraphManagementInstance();
        configManagementGraph.createConfiguration(config);
    }

    /**
     * Create a template configuration according to the supplied {@link Configuration}; if
     * you already created a template configuration or the supplied {@link Configuration}
     * contains the property "graph.graphname", we throw a {@link RuntimeException}; you can then use
     * this template configuration to create a graph using the
     * ConfiguredGraphFactory create signature and supplying a new graphName.
     */
    public static void createTemplateConfiguration(final Configuration config) {
        final ConfigurationManagementGraph configManagementGraph = getConfigGraphManagementInstance();
        configManagementGraph.createTemplateConfiguration(config);
    }

    /**
     * Update configuration corresponding to supplied graphName; we update supplied existing
     * properties and add new ones to the {@link Configuration}; The supplied {@link Configuration} must include a
     * property "graph.graphname" and it must match supplied graphName;
     * NOTE: The updated configuration is only guaranteed to take effect if the {@link Graph} corresponding to
     * graphName has been closed and reopened on every JanusGraph Node.
     */
    public static void updateConfiguration(final String graphName, final Configuration config) {
        final ConfigurationManagementGraph configManagementGraph = getConfigGraphManagementInstance();
        try {
            final JanusGraph graph = open(graphName);
            removeGraphFromCache(graph);
        } catch (Exception e) {
            // cannot open graph, do nothing
            log.error(String.format("Failed to open graph %s with the following error:\n %s.\n" +
            "Thus, it and its traversal will not be bound on this server.", graphName, e.toString()));
        }
        configManagementGraph.updateConfiguration(graphName, config);
    }

    /**
     * Update template configuration by updating supplied existing properties and adding new ones to the
     * {@link Configuration}; your updated Configuration may not contain the property "graph.graphname";
     * NOTE: Any graph using a configuration that was created using the template configuration must--
     * 1) be closed and reopened on every JanusGraph Node 2) have its corresponding Configuration removed
     * and 3) recreate the graph-- before the update is guaranteed to take effect.
     */
    public static void updateTemplateConfiguration(final Configuration config) {
        final ConfigurationManagementGraph configManagementGraph = getConfigGraphManagementInstance();
        configManagementGraph.updateTemplateConfiguration(config);
    }

    /**
     * Remove Configuration according to graphName
     */
    public static void removeConfiguration(final String graphName) {
        final ConfigurationManagementGraph configManagementGraph = getConfigGraphManagementInstance();
        try {
            final JanusGraph graph = open(graphName);
            removeGraphFromCache(graph);
        } catch (Exception e) {
            // cannot open graph, do nothing
            log.error(String.format("Failed to open graph %s with the following error:\n %s.\n" +
            "Thus, it and its traversal will not be bound on this server.", graphName, e.toString()));
        }
        configManagementGraph.removeConfiguration(graphName);
    }

    private static void removeGraphFromCache(final JanusGraph graph) {
        final JanusGraphManager jgm = JanusGraphManagerUtility.getInstance();
        Preconditions.checkState(jgm != null, JANUS_GRAPH_MANAGER_EXPECTED_STATE_MSG);
        jgm.removeGraph(((StandardJanusGraph) graph).getGraphName());
        final ManagementSystem mgmt = (ManagementSystem) graph.openManagement();
        mgmt.evictGraphFromCache();
        mgmt.commit();
    }

    /**
     * Remove template configuration
     */
    public static void removeTemplateConfiguration() {
        final ConfigurationManagementGraph configManagementGraph = getConfigGraphManagementInstance();
        configManagementGraph.removeTemplateConfiguration();
    }

    /**
     * Get Configuration according to supplied graphName mapped to a specific
     * {@link Graph}; if does not exist, return null.
     *
     * @return Map&lt;String, Object&gt;
     */
    public static Map<String, Object> getConfiguration(final String configName) {
        final ConfigurationManagementGraph configManagementGraph = getConfigGraphManagementInstance();
        return configManagementGraph.getConfiguration(configName);
    }

    /**
     * Get a list of all Configurations, excluding the template configuration; if none exist,
     * return an empty list
     *
     * @return List&lt;Map&lt;String, Object&gt;&gt;
     */
    public static List<Map<String, Object>> getConfigurations() {
        final ConfigurationManagementGraph configManagementGraph = getConfigGraphManagementInstance();
        return configManagementGraph.getConfigurations();
    }

    /**
     * Get template configuration if exists, else return null.
     *
     * @return Map&lt;String, Object&gt;
     */
    public static Map<String, Object> getTemplateConfiguration() {
        final ConfigurationManagementGraph configManagementGraph = getConfigGraphManagementInstance();
        return configManagementGraph.getTemplateConfiguration();
    }
}

