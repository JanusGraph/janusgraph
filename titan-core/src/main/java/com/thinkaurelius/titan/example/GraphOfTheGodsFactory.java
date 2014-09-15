package com.thinkaurelius.titan.example;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
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
        config.set("storage.backend", "berkeleyje");
        config.set("storage.directory", directory);
        config.set("index."+INDEX_NAME+".backend","elasticsearch");
        config.set("index." + INDEX_NAME + ".directory", directory + File.separator + "es");
        config.set("index."+INDEX_NAME+".elasticsearch.local-mode",true);
        config.set("index."+INDEX_NAME+".elasticsearch.client-only",false);

        TitanGraph graph = config.open();
        GraphOfTheGodsFactory.load(graph);
        return graph;
    }

    public static void load(final TitanGraph graph) {
        //Create Schema
        TitanManagement mgmt = graph.getManagementSystem();
        final PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        TitanGraphIndex namei = mgmt.buildIndex("name",Vertex.class).addKey(name).unique().buildCompositeIndex();
        mgmt.setConsistency(namei, ConsistencyModifier.LOCK);
        final PropertyKey age = mgmt.makePropertyKey("age").dataType(Integer.class).make();
        mgmt.buildIndex("vertices",Vertex.class).addKey(age).buildMixedIndex(INDEX_NAME);

        final PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).make();
        final PropertyKey reason = mgmt.makePropertyKey("reason").dataType(String.class).make();
        final PropertyKey place = mgmt.makePropertyKey("place").dataType(Geoshape.class).make();
        TitanGraphIndex eindex = mgmt.buildIndex("edges",Edge.class)
                .addKey(reason).addKey(place).buildMixedIndex(INDEX_NAME);

        mgmt.makeEdgeLabel("father").multiplicity(Multiplicity.MANY2ONE).make();
        mgmt.makeEdgeLabel("mother").multiplicity(Multiplicity.MANY2ONE).make();
        EdgeLabel battled = mgmt.makeEdgeLabel("battled").signature(time).make();
        mgmt.buildEdgeIndex(battled, "battlesByTime", Direction.BOTH, Order.DESC, time);
        mgmt.makeEdgeLabel("lives").signature(reason).make();
        mgmt.makeEdgeLabel("pet").make();
        mgmt.makeEdgeLabel("brother").make();

        mgmt.makeVertexLabel("titan").make();
        mgmt.makeVertexLabel("location").make();
        mgmt.makeVertexLabel("god").make();
        mgmt.makeVertexLabel("demigod").make();
        mgmt.makeVertexLabel("human").make();
        mgmt.makeVertexLabel("monster").make();

        mgmt.commit();

        TitanTransaction tx = graph.newTransaction();
        // vertices

        Vertex saturn = tx.addVertexWithLabel("titan");
        saturn.setProperty("name", "saturn");
        saturn.setProperty("age", 10000);

        Vertex sky = tx.addVertexWithLabel("location");
        ElementHelper.setProperties(sky, "name", "sky");

        Vertex sea = tx.addVertexWithLabel("location");
        ElementHelper.setProperties(sea, "name", "sea");

        Vertex jupiter = tx.addVertexWithLabel("god");
        ElementHelper.setProperties(jupiter, "name", "jupiter", "age", 5000);

        Vertex neptune = tx.addVertexWithLabel("god");
        ElementHelper.setProperties(neptune, "name", "neptune", "age", 4500);

        Vertex hercules = tx.addVertexWithLabel("demigod");
        ElementHelper.setProperties(hercules, "name", "hercules", "age", 30);

        Vertex alcmene = tx.addVertexWithLabel("human");
        ElementHelper.setProperties(alcmene, "name", "alcmene", "age", 45);

        Vertex pluto = tx.addVertexWithLabel("god");
        ElementHelper.setProperties(pluto, "name", "pluto", "age", 4000);

        Vertex nemean = tx.addVertexWithLabel("monster");
        ElementHelper.setProperties(nemean, "name", "nemean");

        Vertex hydra = tx.addVertexWithLabel("monster");
        ElementHelper.setProperties(hydra, "name", "hydra");

        Vertex cerberus = tx.addVertexWithLabel("monster");
        ElementHelper.setProperties(cerberus, "name", "cerberus");

        Vertex tartarus = tx.addVertexWithLabel("location");
        ElementHelper.setProperties(tartarus, "name", "tartarus");

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
        tx.commit();
    }

    /**
     * Calls {@link TitanFactory#open(String)}, passing the Titan configuration file path
     * which must be the sole element in the {@code args} array, then calls
     * {@link #load(com.thinkaurelius.titan.core.TitanGraph)} on the opened graph,
     * then calls {@link com.thinkaurelius.titan.core.TitanGraph#shutdown()}
     * and returns.
     * <p>
     * This method may call {@link System#exit(int)} if it encounters an error, such as
     * failure to parse its arguments.  Only use this method when executing main from
     * a command line.  Use one of the other methods on this class ({@link #create(String)}
     * or {@link #load(com.thinkaurelius.titan.core.TitanGraph)}) when calling from
     * an enclosing application.
     *
     * @param args a singleton array containing a path to a Titan config properties file
     */
    public static void main(String args[]) {
        if (null == args || 1 != args.length) {
            System.err.println("Usage: GraphOfTheGodsFactory <titan-config-file>");
            System.exit(1);
        }

        TitanGraph g = TitanFactory.open(args[0]);
        load(g);
        g.shutdown();
    }
}
