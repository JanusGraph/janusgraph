package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.VertexLabel;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.graphdb.TitanGraphBaseTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.gremlin.AbstractGraphProvider;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class AbstractTitanGraphProvider extends AbstractGraphProvider {

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        if (null != g) {
            if (g.features().graph().supportsTransactions())
                g.tx().rollback();
            g.close();
        }

        WriteConfiguration config = new CommonsConfiguration(configuration);
        BasicConfiguration readConfig = new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,config, BasicConfiguration.Restriction.NONE);
        if (readConfig.has(GraphDatabaseConfiguration.STORAGE_BACKEND)) {
            TitanGraphBaseTest.clearGraph(config);
        }
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName) {
        BasicConfiguration conf = getTitanConfiguration(graphName,test,testMethodName);
        Map<String,Object> result = new HashMap<>();
        conf.getAll().entrySet().stream().forEach( e -> result.put(ConfigElement.getPath(e.getKey().element, e.getKey().umbrellaElements),e.getValue()));
        result.put(Graph.GRAPH, TitanGraph.class.getName());
        return result;
    }

    public abstract BasicConfiguration getTitanConfiguration(String graphName, Class<?> test, String testMethodName);

    @Override
    public void loadGraphData(final Graph g, final LoadGraphWith loadGraphWith) {
        this.createIndices((TitanGraph) g, loadGraphWith.value());
        super.loadGraphData(g, loadGraphWith);
    }

    private void createIndices(final TitanGraph g, final LoadGraphWith.GraphData graphData) {
        final Random random = new Random();
        if (!random.nextBoolean()) return;

        TitanManagement mgmt = g.openManagement();
        if (graphData.equals(LoadGraphWith.GraphData.GRATEFUL)) {
            VertexLabel artist = mgmt.makeVertexLabel("artist").make();
            VertexLabel song = mgmt.makeVertexLabel("song").make();

            PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
            PropertyKey songType = mgmt.makePropertyKey("songType").dataType(String.class).make();
            PropertyKey performances = mgmt.makePropertyKey("performances").dataType(Integer.class).make();

            mgmt.buildIndex("artistByName",Vertex.class).addKey(name).indexOnly(artist).buildCompositeIndex();
            mgmt.buildIndex("songByName",Vertex.class).addKey(name).indexOnly(song).buildCompositeIndex();
            mgmt.buildIndex("songByType",Vertex.class).addKey(songType).indexOnly(song).buildCompositeIndex();
            mgmt.buildIndex("songByPerformances",Vertex.class).addKey(performances).indexOnly(song).buildCompositeIndex();

        } else if (graphData.equals(LoadGraphWith.GraphData.MODERN)) {
            VertexLabel person = mgmt.makeVertexLabel("person").make();
            VertexLabel software = mgmt.makeVertexLabel("software").make();

            PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
            PropertyKey lang = mgmt.makePropertyKey("lang").dataType(String.class).make();
            PropertyKey age = mgmt.makePropertyKey("age").dataType(Integer.class).make();

            mgmt.buildIndex("personByName",Vertex.class).addKey(name).indexOnly(person).buildCompositeIndex();
            mgmt.buildIndex("softwareByName",Vertex.class).addKey(name).indexOnly(software).buildCompositeIndex();
            mgmt.buildIndex("personByAge",Vertex.class).addKey(age).indexOnly(person).buildCompositeIndex();
            mgmt.buildIndex("softwareByLang",Vertex.class).addKey(lang).indexOnly(software).buildCompositeIndex();

        } else if (graphData.equals(LoadGraphWith.GraphData.CLASSIC)) {
            PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
            PropertyKey lang = mgmt.makePropertyKey("lang").dataType(String.class).make();
            PropertyKey age = mgmt.makePropertyKey("age").dataType(Integer.class).make();

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


}
