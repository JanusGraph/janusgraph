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
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.ConfiguredGraphFactory;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.janusgraph.graphdb.management.utils.JanusGraphManagerException;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import java.util.function.Function;
import java.util.Set;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.script.SimpleBindings;
import javax.script.Bindings;

/**
 * This class adheres to the TinkerPop graphManager specifications. It provides a coordinated
 * mechanism by which to instantiate graph references on a given JanusGraph node and a graph
 * reference tracker (or graph cache). Any graph created using the property \"graph.graphname\" and
 * any graph defined at server start, i.e. in the server's YAML file, will go through this
 * JanusGraphManager.
 */
public class JanusGraphManager implements GraphManager {

    private static final Logger log =
        LoggerFactory.getLogger(JanusGraphManager.class);
    public static final String JANUS_GRAPH_MANAGER_EXPECTED_STATE_MSG
            = "Gremlin Server must be configured to use the JanusGraphManager.";

    private final Map<String, Graph> graphs = new ConcurrentHashMap<>();
    private final Map<String, TraversalSource> traversalSources = new ConcurrentHashMap<>();
    private final Object instantiateGraphLock = new Object();
    private GremlinExecutor gremlinExecutor = null;

    private static JanusGraphManager instance = null;
    private static final String CONFIGURATION_MANAGEMENT_GRAPH_KEY = ConfigurationManagementGraph.class.getSimpleName();

    /**
     * This class adheres to the TinkerPop graphManager specifications. It provides a coordinated
     * mechanism by which to instantiate graph references on a given JanusGraph node and a graph
     * reference tracker (or graph cache). Any graph created using the property \"graph.graphname\" and
     * any graph defined at server start, i.e. in the server's YAML file, will go through this
     * JanusGraphManager.
     */
    public JanusGraphManager(Settings settings) {
        initialize();
        // Open graphs defined at server start in settings.graphs
        settings.graphs.forEach((key, value) -> {
            final StandardJanusGraph graph = (StandardJanusGraph) JanusGraphFactory.open(value, key);
            if (key.toLowerCase().equals(CONFIGURATION_MANAGEMENT_GRAPH_KEY.toLowerCase())) {
                new ConfigurationManagementGraph(graph);
            }
        });
    }

    private synchronized void initialize() {
        if (null != instance) {
            final String errMsg = "You may not instantiate a JanusGraphManager. The single instance should be handled by Tinkerpop's GremlinServer startup processes.";
            throw new JanusGraphManagerException(errMsg);
        }
        instance = this;
    }

    public static JanusGraphManager getInstance() {
        return instance;
    }

    // To be used for testing purposes only, so we can run tests in parallel
    public static JanusGraphManager getInstance(boolean forceCreate) {
        if (forceCreate) {
            return new JanusGraphManager(new Settings());
        } else {
            return instance;
        }
    }

    public void configureGremlinExecutor(GremlinExecutor gremlinExecutor) {
        this.gremlinExecutor = gremlinExecutor;
        final ScheduledExecutorService bindExecutor = Executors.newScheduledThreadPool(1);
        // Dynamically created graphs created with the ConfiguredGraphFactory are
        // bound across all nodes in the cluster and in the face of server restarts
        bindExecutor.scheduleWithFixedDelay(new GremlinExecutorGraphBinder(this.gremlinExecutor), 0, 20L, TimeUnit.SECONDS);
    }

    private class GremlinExecutorGraphBinder implements Runnable {
        final GremlinExecutor gremlinExecutor;

        public GremlinExecutorGraphBinder(GremlinExecutor gremlinExecutor) {
            this.gremlinExecutor = gremlinExecutor;
        }

        @Override
        public void run() {
            ConfiguredGraphFactory.getGraphNames().forEach(it -> {
                try {
                    final Graph graph = ConfiguredGraphFactory.open(it);
                    this.gremlinExecutor.getScriptEngineManager().put(it, graph);
                    this.gremlinExecutor.getScriptEngineManager().put(it + "_traversal", graph.traversal());
                } catch (Exception e) {
                    // cannot open graph, do nothing
                    log.error(String.format("Failed to open graph %s with the following error:\n %s.\n" +
                    "Thus, it and its traversal will not be bound on this server.", it, e.toString()));
                }
            });
        }
    }

    // To be used for testing purposes
    protected static void shutdownJanusGraphManager() {
        instance = null;
    }

    /**
     * @deprecated
     */
    @Deprecated
    @Override
    public Map getGraphs() {
        return graphs;
    }

    @Override
    public Set<String> getGraphNames() {
        return graphs.keySet();
    }

    @Override
    public Graph getGraph(String gName) {
        return graphs.get(gName);
    }

    @Override
    public void putGraph(String gName, Graph g) {
        graphs.put(gName, g);
    }

    /**
     * @deprecated
     */
    @Override
    @Deprecated
    public Map<String, TraversalSource> getTraversalSources() {
        return traversalSources;
    }

    @Override
    public Set<String> getTraversalSourceNames() {
        return traversalSources.keySet();
    }

    @Override
    public TraversalSource getTraversalSource(String tsName) {
        return traversalSources.get(tsName);
    }

    @Override
    public void putTraversalSource(String tsName, TraversalSource ts) {
        traversalSources.put(tsName, ts);
    }

    @Override
    public TraversalSource removeTraversalSource(String tsName) {
        if (tsName == null) return null;
        return traversalSources.remove(tsName);
    }

    @Override
    public Bindings getAsBindings() {
        final Bindings bindings = new SimpleBindings();
        graphs.forEach(bindings::put);
        return bindings;
    }

    @Override
    public void rollbackAll() {
        graphs.forEach((key, graph) -> {
            if (graph.tx().isOpen()) {
                graph.tx().rollback();
            }
        });
    }

    @Override
    public void rollback(final Set<String> graphSourceNamesToCloseTxOn) {
        commitOrRollback(graphSourceNamesToCloseTxOn, false);
    }

    @Override
    public void commitAll() {
        graphs.forEach((key, graph) -> {
            if (graph.tx().isOpen())
                graph.tx().commit();
        });
    }

    @Override
    public void commit(final Set<String> graphSourceNamesToCloseTxOn) {
        commitOrRollback(graphSourceNamesToCloseTxOn, true);
    }

    public void commitOrRollback(Set<String> graphSourceNamesToCloseTxOn, Boolean commit) {
        graphSourceNamesToCloseTxOn.forEach(e -> {
            final Graph graph = getGraph(e);
            if (null != graph) {
                closeTx(graph, commit);
            }
        });
    }

    public void closeTx(Graph graph, Boolean commit) {
        if (graph.tx().isOpen()) {
            if (commit) {
                graph.tx().commit();
            } else {
                graph.tx().rollback();
            }
        }
    }

    @Override
    public Graph openGraph(String gName, Function<String, Graph> thunk) {
        Graph graph = graphs.get(gName);
        if (graph != null && !((StandardJanusGraph) graph).isClosed()) {
            if (null != gremlinExecutor) {
                this.gremlinExecutor.getScriptEngineManager().put(gName, graph);
                this.gremlinExecutor.getScriptEngineManager().put(gName + "_traversal", graph.traversal());
            }
            return graph;
        } else {
            synchronized (instantiateGraphLock) {
                graph = graphs.get(gName);
                if (graph == null || ((StandardJanusGraph) graph).isClosed()) {
                    graph = thunk.apply(gName);
                    graphs.put(gName, graph);
                }
            }
            if (null != gremlinExecutor) {
                this.gremlinExecutor.getScriptEngineManager().put(gName, graph);
                this.gremlinExecutor.getScriptEngineManager().put(gName + "_traversal", graph.traversal());
            }
            return graph;
        }
    }

    @Override
    public Graph removeGraph(String gName) {
        if (gName == null) return null;
        return graphs.remove(gName);
    }
}

