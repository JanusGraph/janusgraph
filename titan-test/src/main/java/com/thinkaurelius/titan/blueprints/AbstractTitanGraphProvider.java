package com.thinkaurelius.titan.blueprints;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.*;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.graphdb.TitanGraphBaseTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import com.tinkerpop.gremlin.AbstractGraphProvider;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.BatchTest;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.kryo.GremlinKryo;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;
import org.apache.commons.configuration.Configuration;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class AbstractTitanGraphProvider extends AbstractGraphProvider {

    @Override
    public GremlinKryo createConfiguredGremlinKryo() {
        return GremlinKryo.build()
                .addCustom(RelationIdentifier.class)
                .create();
    }

    @Override
    public void clear(Graph g, final Configuration configuration) throws Exception {
        if (null != g) {
            while (g instanceof WrappedGraph) g = ((WrappedGraph<? extends Graph>)g).getBaseGraph();
            TitanGraph graph = (TitanGraph)g;
            if (graph.isOpen()) {
                g.tx().rollback();
                g.close();
            }
        }

        WriteConfiguration config = new CommonsConfiguration(configuration);
        BasicConfiguration readConfig = new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,config, BasicConfiguration.Restriction.NONE);
        if (readConfig.has(GraphDatabaseConfiguration.STORAGE_BACKEND)) {
            TitanGraphBaseTest.clearGraph(config);
        }
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName) {
        ModifiableConfiguration conf = getTitanConfiguration(graphName,test,testMethodName);
        initializeSchema(conf,test,testMethodName);
        Map<String,Object> result = new HashMap<>();
        conf.getAll().entrySet().stream().forEach( e -> result.put(ConfigElement.getPath(e.getKey().element, e.getKey().umbrellaElements),e.getValue()));
        result.put(Graph.GRAPH, TitanFactory.class.getName());
        return result;
    }

    public abstract ModifiableConfiguration getTitanConfiguration(String graphName, Class<?> test, String testMethodName);

    @Override
    public void loadGraphData(final Graph g, final LoadGraphWith loadGraphWith) {
        this.createIndices((TitanGraph) g, loadGraphWith.value());
        super.loadGraphData(g, loadGraphWith);
    }

    private void createIndices(final TitanGraph g, final LoadGraphWith.GraphData graphData) {

        TitanManagement mgmt = g.openManagement();
        if (graphData.equals(LoadGraphWith.GraphData.GRATEFUL)) {
            VertexLabel artist = mgmt.makeVertexLabel("artist").make();
            VertexLabel song = mgmt.makeVertexLabel("song").make();

            PropertyKey name = mgmt.makePropertyKey("name").cardinality(Cardinality.LIST).dataType(String.class).make();
            PropertyKey songType = mgmt.makePropertyKey("songType").cardinality(Cardinality.LIST).dataType(String.class).make();
            PropertyKey performances = mgmt.makePropertyKey("performances").cardinality(Cardinality.LIST).dataType(Integer.class).make();

            mgmt.buildIndex("artistByName",Vertex.class).addKey(name).indexOnly(artist).buildCompositeIndex();
            mgmt.buildIndex("songByName",Vertex.class).addKey(name).indexOnly(song).buildCompositeIndex();
            mgmt.buildIndex("songByType",Vertex.class).addKey(songType).indexOnly(song).buildCompositeIndex();
            mgmt.buildIndex("songByPerformances",Vertex.class).addKey(performances).indexOnly(song).buildCompositeIndex();

        } else if (graphData.equals(LoadGraphWith.GraphData.MODERN)) {
            VertexLabel person = mgmt.makeVertexLabel("person").make();
            VertexLabel software = mgmt.makeVertexLabel("software").make();

            PropertyKey name = mgmt.makePropertyKey("name").cardinality(Cardinality.LIST).dataType(String.class).make();
            PropertyKey lang = mgmt.makePropertyKey("lang").cardinality(Cardinality.LIST).dataType(String.class).make();
            PropertyKey age = mgmt.makePropertyKey("age").cardinality(Cardinality.LIST).dataType(Integer.class).make();

            mgmt.buildIndex("personByName",Vertex.class).addKey(name).indexOnly(person).buildCompositeIndex();
            mgmt.buildIndex("softwareByName",Vertex.class).addKey(name).indexOnly(software).buildCompositeIndex();
            mgmt.buildIndex("personByAge",Vertex.class).addKey(age).indexOnly(person).buildCompositeIndex();
            mgmt.buildIndex("softwareByLang",Vertex.class).addKey(lang).indexOnly(software).buildCompositeIndex();

        } else if (graphData.equals(LoadGraphWith.GraphData.CLASSIC)) {
            PropertyKey name = mgmt.makePropertyKey("name").cardinality(Cardinality.LIST).dataType(String.class).make();
            PropertyKey lang = mgmt.makePropertyKey("lang").cardinality(Cardinality.LIST).dataType(String.class).make();
            PropertyKey age = mgmt.makePropertyKey("age").cardinality(Cardinality.LIST).dataType(Integer.class).make();

            mgmt.buildIndex("byName",Vertex.class).addKey(name).buildCompositeIndex();
            mgmt.buildIndex("byAge",Vertex.class).addKey(age).buildCompositeIndex();
            mgmt.buildIndex("byLang",Vertex.class).addKey(lang).buildCompositeIndex();

        } else {
            // TODO: add CREW work here.
            // TODO: add meta_property indices when meta_property graph is provided
            //throw new RuntimeException("Could not load graph with " + graphData);
        }
        mgmt.commit();
    }

    private void initializeSchema(ModifiableConfiguration conf, Class<?> test, String testMethodName) {
        conf.set(GraphDatabaseConfiguration.AUTO_TYPE,"tp3");
    }

    public static class Tp3TestSchema implements DefaultSchemaMaker {

        private final Set<String> multiProperties = ImmutableSet.of("age","name");

        @Override
        public EdgeLabel makeEdgeLabel(EdgeLabelMaker factory) {
            return factory.make();
        }

        @Override
        public PropertyKey makePropertyKey(PropertyKeyMaker factory) {
            System.out.println("Auto-maker: " + factory.getName());
            if (multiProperties.contains(factory.getName().toLowerCase())) {
                factory.cardinality(Cardinality.LIST);
            }
            factory.dataType(Object.class);
            return factory.make();
        }

        @Override
        public VertexLabel makeVertexLabel(VertexLabelMaker factory) {
            return factory.make();
        }

        @Override
        public boolean ignoreUndefinedQueryTypes() {
            return true;
        }
    }

}
