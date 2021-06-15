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

package org.janusgraph.blueprints;

import com.google.common.collect.Sets;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.TransactionTest;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.JanusGraphBaseTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.olap.computer.FulgoraElementTraversal;
import org.janusgraph.graphdb.olap.computer.FulgoraVertexProperty;
import org.janusgraph.graphdb.relations.CacheEdge;
import org.janusgraph.graphdb.relations.CacheVertexProperty;
import org.janusgraph.graphdb.relations.SimpleJanusGraphProperty;
import org.janusgraph.graphdb.relations.StandardEdge;
import org.janusgraph.graphdb.relations.StandardVertexProperty;
import org.janusgraph.graphdb.tinkerpop.JanusGraphVariables;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.VertexLabelVertex;
import org.janusgraph.graphdb.types.system.EmptyVertex;
import org.janusgraph.graphdb.types.vertices.EdgeLabelVertex;
import org.janusgraph.graphdb.types.vertices.JanusGraphSchemaVertex;
import org.janusgraph.graphdb.types.vertices.PropertyKeyVertex;
import org.janusgraph.graphdb.vertices.CacheVertex;
import org.janusgraph.graphdb.vertices.PreloadedVertex;
import org.janusgraph.graphdb.vertices.StandardVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class AbstractJanusGraphProvider extends AbstractGraphProvider {

    private static final Logger logger = LoggerFactory.getLogger(AbstractJanusGraphProvider.class);

    private static final Set<Class> IMPLEMENTATION = Sets.newHashSet(
        StandardJanusGraph.class,
        StandardJanusGraphTx.class,

        StandardVertex.class,
        CacheVertex.class,
        PreloadedVertex.class,
        EdgeLabelVertex.class,
        PropertyKeyVertex.class,
        VertexLabelVertex.class,
        JanusGraphSchemaVertex.class,
        EmptyVertex.class,

        StandardEdge.class,
        CacheEdge.class,
        EdgeLabel.class,
        EdgeLabelVertex.class,

        StandardVertexProperty.class,
        CacheVertexProperty.class,
        SimpleJanusGraphProperty.class,
        CacheVertexProperty.class,
        FulgoraVertexProperty.class,

        JanusGraphVariables.class,

        FulgoraElementTraversal.class);

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATION;
    }

    @Override
    public GraphTraversalSource traversal(final Graph graph) {
        return graph.traversal();
    }

    @Override
    public GraphTraversalSource traversal(final Graph graph, final TraversalStrategy... strategies) {
        return graph.traversal().withStrategies(strategies);
    }


    @Override
    public void clear(Graph g, final Configuration configuration) throws Exception {
        if (null != g) {
            while (g instanceof WrappedGraph) g = ((WrappedGraph<? extends Graph>) g).getBaseGraph();
            JanusGraph graph = (JanusGraph) g;
            if (graph.isOpen()) {
                if (g.tx().isOpen()) g.tx().rollback();
                try {
                    g.close();
                } catch (IOException | IllegalStateException e) {
                    logger.warn("Titan graph may not have closed cleanly", e);
                }
            }
        }

        WriteConfiguration config = new CommonsConfiguration(configuration);
        BasicConfiguration readConfig = new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS, config,
            BasicConfiguration.Restriction.NONE);
        if (readConfig.has(GraphDatabaseConfiguration.STORAGE_BACKEND)) {
            JanusGraphBaseTest.clearGraph(config);
        }
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName,
                                                    final LoadGraphWith.GraphData loadGraphWith) {
        ModifiableConfiguration conf = getJanusGraphConfiguration(graphName, test, testMethodName);
        conf.set(GraphDatabaseConfiguration.COMPUTER_RESULT_MODE, "persist");
        conf.set(GraphDatabaseConfiguration.AUTO_TYPE, "tp3");
        Map<String, Object> result = new HashMap<>();
        conf.getAll().forEach(
                (key, value) -> result.put(ConfigElement.getPath(key.element, key.umbrellaElements), value));
        result.put(Graph.GRAPH, JanusGraphFactory.class.getName());
        return result;
    }

    public abstract ModifiableConfiguration getJanusGraphConfiguration(String graphName, Class<?> test,
                                                                       String testMethodName);

    @Override
    public void loadGraphData(final Graph g, final LoadGraphWith loadGraphWith, final Class testClass,
                              final String testName) {
        if (loadGraphWith != null) {
            this.createIndices((JanusGraph) g, loadGraphWith.value());
        } else {
            if (TransactionTest.class.equals(testClass)
                    && testName.equalsIgnoreCase("shouldExecuteWithCompetingThreads")) {
                JanusGraphManagement management = ((JanusGraph) g).openManagement();
                management.makePropertyKey("blah").dataType(Double.class).make();
                management.makePropertyKey("bloop").dataType(Integer.class).make();
                management.makePropertyKey("test").dataType(Object.class).make();
                management.makeEdgeLabel("friend").make();
                management.commit();
            }
        }
        super.loadGraphData(g, loadGraphWith, testClass, testName);
    }

    private void createIndices(final JanusGraph g, final LoadGraphWith.GraphData graphData) {
        JanusGraphManagement management = g.openManagement();
        if (graphData.equals(LoadGraphWith.GraphData.GRATEFUL)) {
            VertexLabel artist = management.makeVertexLabel("artist").make();
            VertexLabel song = management.makeVertexLabel("song").make();

            PropertyKey name = management.makePropertyKey("name").cardinality(Cardinality.LIST)
                    .dataType(String.class).make();
            PropertyKey songType = management.makePropertyKey("songType").cardinality(Cardinality.LIST)
                    .dataType(String.class).make();
            PropertyKey performances = management.makePropertyKey("performances").cardinality(Cardinality.LIST)
                    .dataType(Integer.class).make();

            management.buildIndex("artistByName", Vertex.class).addKey(name).indexOnly(artist)
                    .buildCompositeIndex();
            management.buildIndex("songByName", Vertex.class).addKey(name).indexOnly(song)
                    .buildCompositeIndex();
            management.buildIndex("songByType", Vertex.class).addKey(songType).indexOnly(song)
                    .buildCompositeIndex();
            management.buildIndex("songByPerformances", Vertex.class).addKey(performances).indexOnly(song)
                    .buildCompositeIndex();

        } else if (graphData.equals(LoadGraphWith.GraphData.MODERN)) {
            VertexLabel person = management.makeVertexLabel("person").make();
            VertexLabel software = management.makeVertexLabel("software").make();

            PropertyKey name = management.makePropertyKey("name").cardinality(Cardinality.LIST)
                    .dataType(String.class).make();
            PropertyKey lang = management.makePropertyKey("lang").cardinality(Cardinality.LIST)
                    .dataType(String.class).make();
            PropertyKey age = management.makePropertyKey("age").cardinality(Cardinality.LIST)
                    .dataType(Integer.class).make();

            management.buildIndex("personByName", Vertex.class).addKey(name).indexOnly(person)
                    .buildCompositeIndex();
            management.buildIndex("softwareByName", Vertex.class).addKey(name).indexOnly(software)
                    .buildCompositeIndex();
            management.buildIndex("personByAge", Vertex.class).addKey(age).indexOnly(person)
                    .buildCompositeIndex();
            management.buildIndex("softwareByLang", Vertex.class).addKey(lang).indexOnly(software)
                    .buildCompositeIndex();

        } else if (graphData.equals(LoadGraphWith.GraphData.CLASSIC)) {
            PropertyKey name = management.makePropertyKey("name").cardinality(Cardinality.LIST)
                    .dataType(String.class).make();
            PropertyKey lang = management.makePropertyKey("lang").cardinality(Cardinality.LIST)
                    .dataType(String.class).make();
            PropertyKey age = management.makePropertyKey("age").cardinality(Cardinality.LIST)
                    .dataType(Integer.class).make();

            management.buildIndex("byName", Vertex.class).addKey(name).buildCompositeIndex();
            management.buildIndex("byAge", Vertex.class).addKey(age).buildCompositeIndex();
            management.buildIndex("byLang", Vertex.class).addKey(lang).buildCompositeIndex();

        } else {
            // TODO: add CREW work here.
            // TODO: add meta_property indices when meta_property graph is provided
            //throw new RuntimeException("Could not load graph with " + graphData);
        }
        management.commit();
    }

    @Override
    public Optional<TestListener> getTestListener() {
        return Optional.of(JanusGraphTestListener.instance());
    }
}
