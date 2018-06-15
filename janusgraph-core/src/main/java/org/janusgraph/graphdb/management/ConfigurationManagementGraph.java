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

package org.janusgraph.graphdb.management;

import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.management.utils.ConfigurationManagementGraphNotEnabledException;
import org.janusgraph.graphdb.management.utils.ConfigurationManagementGraphAlreadyInstantiatedException;
import static org.janusgraph.core.schema.SchemaAction.ENABLE_INDEX;
import static org.janusgraph.core.schema.SchemaStatus.INSTALLED;
import static org.janusgraph.core.schema.SchemaStatus.REGISTERED;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * This class allows you to create/update/remove configuration objects used to open a graph.
 * To use these APIs, you must define one of your graph's key as "ConfigurationManagementGraph"
 * in your server's YAML; the configuration objects you define using these APIs will be stored
 * in a graph representation (on a vertex, more precisely), and this graph will be opened
 * according to the file supplied along with the "ConfigurationManagementGraph" key.
 */
public class ConfigurationManagementGraph {

    private static final Logger log = LoggerFactory.getLogger(ConfigurationManagementGraph.class);
    private static ConfigurationManagementGraph instance = null;
    public static final String PROPERTY_GRAPH_NAME = GraphDatabaseConfiguration.GRAPH_NAME.toStringWithoutRoot();
    public static final String PROPERTY_CREATED_USING_TEMPLATE = "Created_Using_Template";
    private static final String VERTEX_LABEL = "Configuration";
    private static final String GRAPH_NAME_INDEX = "Graph_Name_Index";
    private static final String PROPERTY_TEMPLATE = "Template_Configuration";
    private static final String TEMPLATE_INDEX = "Template_Index";

    private final StandardJanusGraph graph;

    /**
     * This class allows you to create/update/remove configuration objects used to open a graph.
     * To use these APIs, you must define one of your graph's key as "ConfigurationManagementGraph"
     * in your server's YAML; the configuration objects you define using these APIs will be stored
     * in a graph representation (on a vertex, more precisely), and this graph will be opened
     * according to the file supplied along with the "ConfigurationManagementGraph" key.
     */
    // Would  prefer this constructor to have no modifier (for instantiation by JanusGraphManager,
    // but for now, leaving open for testing purposes
    public ConfigurationManagementGraph(StandardJanusGraph graph) {
        initialize();
        this.graph = graph;
        createIndexIfDoesNotExist(GRAPH_NAME_INDEX, PROPERTY_GRAPH_NAME, String.class, true);
        createIndexIfDoesNotExist(TEMPLATE_INDEX, PROPERTY_TEMPLATE, Boolean.class, false);
        createIndexIfDoesNotExist(PROPERTY_CREATED_USING_TEMPLATE, PROPERTY_CREATED_USING_TEMPLATE, Boolean.class, false);
    }

    private synchronized void initialize() {
        if (null != instance) {
            final String errMsg = "ConfigurationManagementGraph should be instantiated just once, by the JanusGraphManager.";
            throw new ConfigurationManagementGraphAlreadyInstantiatedException(errMsg);
        }
        instance = this;
    }

    /**
     * If one of your "graphs" key was equivalent to "ConfigurationManagementGraph" in your
     * YAML file supplied at server start, then we return the ConfigurationManagementGraph
     * Singleton-- otherwise we throw a {@link ConfigurationManagementGraphNotEnabledException} exception.
     */
    public static ConfigurationManagementGraph getInstance() throws ConfigurationManagementGraphNotEnabledException {
        if (null == instance) {
            throw new ConfigurationManagementGraphNotEnabledException(
                "Please add a key named \"ConfigurationManagementGraph\" to the \"graphs\" property " +
                "in your YAML file and restart the server to be able to use the functionality " +
                "of the ConfigurationManagementGraph class."
            );
        }

        return instance;
    }

    /**
     * Create a configuration according to the supplied {@link Configuration}; you must include
     * the property "graph.graphname" with a value in the configuration; you can then
     * open your graph using graph.graphname without having to supply the
     * Configuration or File each time using the {@link org.janusgraph.core.ConfiguredGraphFactory}.
     */
    public void createConfiguration(final Configuration config) {
        Preconditions.checkArgument(config.containsKey(PROPERTY_GRAPH_NAME),
                                    String.format("Please include the property \"%s\" in your configuration.",
                                                  PROPERTY_GRAPH_NAME
                                    ));
        final Map<Object, Object> map = ConfigurationConverter.getMap(config);
        final Vertex v = graph.addVertex(T.label, VERTEX_LABEL);
        map.forEach((key, value) -> v.property((String) key, value));
        v.property(PROPERTY_TEMPLATE, false);
        graph.tx().commit();
    }

    /**
     * Create a template configuration according to the supplied {@link Configuration}; if
     * you already created a template configuration or the supplied {@link Configuration}
     * contains the property "graph.graphname", we throw a {@link RuntimeException}; you can then use
     * this template configuration to create a graph using the
     * ConfiguredGraphFactory create signature and supplying a new graphName.
     */
    public void createTemplateConfiguration(final Configuration config) {
        Preconditions.checkArgument(!config.containsKey(PROPERTY_GRAPH_NAME),
                                    String.format("Your template configuration may not contain the property \"%s\".",
                                                  PROPERTY_GRAPH_NAME
                                    ));
        Preconditions.checkState(null == getTemplateConfiguration(),
                                "You may only have one template configuration and one exists already.");
        final Map<Object, Object> map = ConfigurationConverter.getMap(config);
        final Vertex v = graph.addVertex();
        v.property(PROPERTY_TEMPLATE, true);
        map.forEach((key, value) -> v.property((String) key, value));
        graph.tx().commit();

    }

    /**
     * Update configuration corresponding to supplied graphName; we update supplied existing
     * properties and add new ones to the {@link Configuration}; The supplied {@link Configuration} must include a
     * property "graph.graphname" and it must match supplied graphName;
     * NOTE: The updated configuration is only guaranteed to take effect if the {@link Graph} corresponding to
     * graphName has been closed and reopened on every JanusGraph Node.
     */
    public void updateConfiguration(final String graphName, final Configuration config) {
        final Map<Object, Object> map = ConfigurationConverter.getMap(config);
        if (config.containsKey(PROPERTY_GRAPH_NAME)) {
            final String graphNameOnConfig = (String) map.get(PROPERTY_GRAPH_NAME);
            Preconditions.checkArgument(graphName.equals(graphNameOnConfig),
                                        String.format("Supplied graphName %s does not match property value supplied on config: %s.",
                                                      graphName,
                                                      graphNameOnConfig
                                        ));
        } else {
            map.put(PROPERTY_GRAPH_NAME, graphName);
        }
        log.warn("Configuration {} is only guaranteed to take effect when graph {} has been closed and reopened on all Janus Graph Nodes.",
            graphName,
            graphName
        );
        updateVertexWithProperties(PROPERTY_GRAPH_NAME, graphName, map);
    }

    /**
     * Update template configuration by updating supplied existing properties and adding new ones to the
 		 * {@link Configuration}; your updated Configuration may not contain the property "graph.graphname";
     * NOTE: Any graph using a configuration that was created using the template configuration must--
     * 1) be closed and reopened on every JanusGraph Node 2) have its corresponding Configuration removed
     * and 3) recreate the graph-- before the update is guaranteed to take effect.
     */
    public void updateTemplateConfiguration(final Configuration config) {
        Preconditions.checkArgument(!config.containsKey(PROPERTY_GRAPH_NAME),
                                    String.format("Your updated template configuration may not contain the property \"%s\".",
                                                  PROPERTY_GRAPH_NAME
                                    ));
        log.warn("Any graph configuration created using the template configuration are only guaranteed to have their configuration updated " +
                 "according to this new template configuration when the graph in question has been closed on every Janus Graph Node, its " +
                 "corresponding Configuration has been removed, and the graph has been recreated.");
        updateVertexWithProperties(PROPERTY_TEMPLATE, true, ConfigurationConverter.getMap(config));
    }


    /**
     * Remove Configuration according to graphName
     */
    public void removeConfiguration(final String graphName) {
        removeVertex(PROPERTY_GRAPH_NAME, graphName);
    }

    /**
     * Remove template configuration
     */
    public void removeTemplateConfiguration() {
        removeVertex(PROPERTY_TEMPLATE, true);
    }

    /**
     * Get Configuration according to supplied graphName mapped to a specific
     * {@link Graph}; if does not exist, return null.
     *
     * @return Map&lt;String, Object&gt;
     */
    public Map<String, Object> getConfiguration(final String configName) {
        final List<Map<String, Object>> graphConfiguration = graph.traversal().V().has(PROPERTY_GRAPH_NAME, configName).valueMap().toList();
        if (graphConfiguration.isEmpty()) return null;
        else if (graphConfiguration.size() > 1) { // this case shouldn't happen because our index has a unique constraint
            log.warn("Your configuration management graph is an a bad state. Please " +
                     "ensure you have just one configuration per graph. The behavior " +
                     "of the class' APIs are henceforth unpredictable until this is fixed.");
        }
        return deserializeVertexProperties(graphConfiguration.get(0));
    }

    /**
     * Get a list of all Configurations, excluding the template configuration; if none exist,
     * return an empty list
     *
     * @return List&lt;Map&lt;String, Object&gt;&gt;
     */
    public List<Map<String, Object>> getConfigurations() {
        final List<Map<String, Object>> graphConfigurations = graph.traversal().V().has(PROPERTY_TEMPLATE, false).valueMap().toList();
        return graphConfigurations.stream().map(this::deserializeVertexProperties).collect(Collectors.toList());
    }

    /**
     * Get template configuration if exists, else return null.
     *
     * @return Map&lt;String, Object&gt;
     */
    public Map<String, Object> getTemplateConfiguration() {
        final List<Map<String, Object>> templateConfigurations = graph.traversal().V().has(PROPERTY_TEMPLATE, true).valueMap().toList();
        if (templateConfigurations.size() == 0) return null;

        if (templateConfigurations.size() > 1) {
            log.warn("Your configuration management graph is an a bad state. Please " +
                     "ensure you have just one template configuration. The behavior " +
                     "of the class' APIs are henceforth unpredictable until this is fixed.");
        }
        templateConfigurations.get(0).remove(PROPERTY_TEMPLATE);
        return deserializeVertexProperties(templateConfigurations.get(0));

    }

    private void removeVertex(String property, Object value) {
        final GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V().has(property, value);
        if (traversal.hasNext()) {
            traversal.next().remove();
            graph.tx().commit();
        }
    }

    private void createIndexIfDoesNotExist(String indexName, String propertyKeyName, Class dataType,boolean unique) {
        graph.tx().rollback();
        JanusGraphManagement management = graph.openManagement();
        if (null == management.getGraphIndex(indexName)) {
            final PropertyKey key = management.makePropertyKey(propertyKeyName).dataType(dataType).make();

            final JanusGraphIndex index;
            if (unique) index = management.buildIndex(indexName, Vertex.class).addKey(key).unique().buildCompositeIndex();
            else index = management.buildIndex(indexName, Vertex.class).addKey(key).buildCompositeIndex();
            try {
                if (index.getIndexStatus(key) == INSTALLED) {
                    management.commit();
                    ManagementSystem.awaitGraphIndexStatus(graph, indexName).call();
                    management = graph.openManagement();
                    management.updateIndex(index, ENABLE_INDEX).get();
                } else if (index.getIndexStatus(key) == REGISTERED) {
                    management.updateIndex(index, ENABLE_INDEX).get();
                }
            } catch (InterruptedException | ExecutionException e) {
                log.warn("Failed to create index {} for ConfigurationManagementGraph with exception: {}",
                        indexName,
                        e.toString()
                );
                management.rollback();
                graph.tx().rollback();
            }
            management.commit();
            graph.tx().commit();
        }
    }

    private void updateVertexWithProperties(String propertyKey, Object propertyValue, Map<Object, Object> map) {
        if (graph.traversal().V().has(propertyKey, propertyValue).hasNext()) {
            final Vertex v = graph.traversal().V().has(propertyKey, propertyValue).next();
            map.forEach((key, value) -> v.property((String) key, value));
            graph.tx().commit();
        }
    }

    private Map<String, Object> deserializeVertexProperties(Map<String, Object> map) {
        map.forEach((key, value) -> {
            if (value instanceof List) {
                if (((List) value).size() > 1) {
                    log.warn("Your configuration management graph is an a bad state. Please " +
                             "ensure each vertex property is not supplied a Collection as a value. The behavior " +
                             "of the class' APIs are henceforth unpredictable until this is fixed.");
                }
                map.put(key, ((List) value).get(0));
            }
        });
        return map;
    }
}

