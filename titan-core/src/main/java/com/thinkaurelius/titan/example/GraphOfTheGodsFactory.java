package com.thinkaurelius.titan.example;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.graphdb.types.StandardEdgeLabelMaker;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;

import java.io.File;


/**
 * Example Graph factory that creates a {@link TitanGraph} based on roman mythology.
 * Used in the documentation examples and tutorials.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphOfTheGodsFactory {

    public static final String INDEX_NAME = "search";


    public static TitanGraph create(final String directory) {
        TitanFactory.Builder config = TitanFactory.build();
        config.set("storage.backend","berkeleyje");
        config.set("storage.directory",directory);
        config.set("index."+INDEX_NAME+".backend","elasticsearch");
        config.set("index."+INDEX_NAME+".local-mode",true);
        config.set("index."+INDEX_NAME+".client-only",false);
        config.set("index." + INDEX_NAME + ".directory", directory + File.separator + "es");

        TitanGraph graph = config.open();
        GraphOfTheGodsFactory.load(graph);
        return graph;
    }

    public static void load(final TitanGraph graph) {
        //Create Schema
        TitanManagement mgmt = graph.getManagementSystem();
        final PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.buildIndex("name",Vertex.class).indexKey(name).unique().buildInternalIndex();
        final PropertyKey age = mgmt.makePropertyKey("age").dataType(Integer.class).make();
        mgmt.makePropertyKey("type").dataType(String.class).make();
        mgmt.buildIndex("vertices",Vertex.class).indexKey(age).buildExternalIndex(INDEX_NAME);

        final PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).make();
        final PropertyKey reason = mgmt.makePropertyKey("reason").dataType(String.class).make();
        final PropertyKey place = mgmt.makePropertyKey("place").dataType(Geoshape.class).make();
        TitanGraphIndex eindex = mgmt.buildIndex("edges",Edge.class)
                .indexKey(reason).indexKey(place).buildExternalIndex(INDEX_NAME);

        mgmt.makeEdgeLabel("father").multiplicity(Multiplicity.MANY2ONE).make();
        mgmt.makeEdgeLabel("mother").multiplicity(Multiplicity.MANY2ONE).make();
        EdgeLabel battled = mgmt.makeEdgeLabel("battled").signature(time).make();
        mgmt.createEdgeIndex(battled,"timeindex", Direction.BOTH,Order.DESC,time);
        mgmt.makeEdgeLabel("lives").signature(reason).make();
        mgmt.makeEdgeLabel("pet").make();
        mgmt.makeEdgeLabel("brother").make();
        mgmt.commit();

        // vertices

        Vertex saturn = graph.addVertex(null);
        saturn.setProperty("name", "saturn");
        saturn.setProperty("age", 10000);
        saturn.setProperty("type", "titan");

        Vertex sky = graph.addVertex(null);
        ElementHelper.setProperties(sky, "name", "sky", "type", "location");

        Vertex sea = graph.addVertex(null);
        ElementHelper.setProperties(sea, "name", "sea", "type", "location");

        Vertex jupiter = graph.addVertex(null);
        ElementHelper.setProperties(jupiter, "name", "jupiter", "age", 5000, "type", "god");

        Vertex neptune = graph.addVertex(null);
        ElementHelper.setProperties(neptune, "name", "neptune", "age", 4500, "type", "god");

        Vertex hercules = graph.addVertex(null);
        ElementHelper.setProperties(hercules, "name", "hercules", "age", 30, "type", "demigod");

        Vertex alcmene = graph.addVertex(null);
        ElementHelper.setProperties(alcmene, "name", "alcmene", "age", 45, "type", "human");

        Vertex pluto = graph.addVertex(null);
        ElementHelper.setProperties(pluto, "name", "pluto", "age", 4000, "type", "god");

        Vertex nemean = graph.addVertex(null);
        ElementHelper.setProperties(nemean, "name", "nemean", "type", "monster");

        Vertex hydra = graph.addVertex(null);
        ElementHelper.setProperties(hydra, "name", "hydra", "type", "monster");

        Vertex cerberus = graph.addVertex(null);
        ElementHelper.setProperties(cerberus, "name", "cerberus", "type", "monster");

        Vertex tartarus = graph.addVertex(null);
        ElementHelper.setProperties(tartarus, "name", "tartarus", "type", "location");

        // edges

        jupiter.addEdge("father", saturn);
        jupiter.addEdge("lives", sky).setProperty("reason", "loves fresh breezes");
        jupiter.addEdge("brother", neptune);
        jupiter.addEdge("brother", pluto);

        neptune.addEdge("lives", sea).setProperty("reason", "loves waves");
        neptune.addEdge("brother", jupiter);
        neptune.addEdge("brother", pluto);

        hercules.addEdge("father", jupiter);
        hercules.addEdge("mother", alcmene);
        ElementHelper.setProperties(hercules.addEdge("battled", nemean), "time", 1, "place", Geoshape.point(38.1f, 23.7f));
        ElementHelper.setProperties(hercules.addEdge("battled", hydra), "time", 2, "place", Geoshape.point(37.7f, 23.9f));
        ElementHelper.setProperties(hercules.addEdge("battled", cerberus), "time", 12, "place", Geoshape.point(39f, 22f));

        pluto.addEdge("brother", jupiter);
        pluto.addEdge("brother", neptune);
        pluto.addEdge("lives", tartarus).setProperty("reason", "no fear of death");
        pluto.addEdge("pet", cerberus);

        cerberus.addEdge("lives", tartarus);

        // commit the transaction to disk
        graph.commit();
    }
}
