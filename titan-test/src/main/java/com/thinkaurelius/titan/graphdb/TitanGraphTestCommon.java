package com.thinkaurelius.titan.graphdb;


import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;

public abstract class TitanGraphTestCommon {

    public Configuration config;
    public StandardTitanGraph graph;
    public TitanTransaction tx;

    public TitanGraphTestCommon(Configuration config) {
        this.config = config;
    }

    @Before
    public void setUp() throws Exception {
        GraphDatabaseConfiguration graphconfig = new GraphDatabaseConfiguration(config);
        graphconfig.getBackend().clearStorage();
        open();
    }

    @After
    public void tearDown() throws Exception {
        close();
    }

    public void open() {
        //System.out.println(GraphDatabaseConfiguration.toString(config));
        graph = (StandardTitanGraph) TitanFactory.open(config);
        //tx = graph.newThreadBoundTransaction();
        tx = graph.newTransaction();
    }

    public void close() {
        if (null != tx && tx.isOpen())
            tx.commit();

        if (null != graph)
            graph.shutdown();
    }

    public void newTx() {
        if (null != tx && tx.isOpen())
            tx.commit();
        //tx = graph.newThreadBoundTransaction();
        tx = graph.newTransaction();
    }

    public void clopen() {
        close();
        open();
    }

    public static int wrapAround(int value, int maxValue) {
        value = value % maxValue;
        if (value < 0) value = value + maxValue;
        return value;
    }

    public TitanLabel makeSimpleEdgeLabel(String name) {
        TypeMaker etmaker = tx.makeType();
        etmaker.name(name);
        return etmaker.makeEdgeLabel();
    }

    public TitanLabel makeKeyedEdgeLabel(String name, TitanKey primary, TitanKey signature) {
        TypeMaker etmaker = tx.makeType();
        TitanLabel relType = etmaker.name(name).
                primaryKey(primary).signature(signature).directed().makeEdgeLabel();
        return relType;
    }

    public TitanKey makeUniqueStringPropertyKey(String name) {
        return tx.makeType().name(name).vertexUnique(Direction.OUT).
                graphUnique().indexed(Vertex.class).dataType(String.class).makePropertyKey();
    }

    public TitanKey makeStringUIDPropertyKey(String name) {
        return tx.makeType().name(name).
                vertexUnique(Direction.OUT).
                graphUnique().indexed(Vertex.class).
                dataType(String.class).
                makePropertyKey();
    }

    public TitanKey makeStringPropertyKey(String name) {
        return tx.makeType().name(name).vertexUnique(Direction.OUT).
                indexed(Vertex.class).
                dataType(String.class).
                makePropertyKey();
    }

    public TitanKey makeUnindexedStringPropertyKey(String name) {
        return tx.makeType().name(name).vertexUnique(Direction.OUT).
                dataType(String.class).
                makePropertyKey();
    }

    public TitanKey makeIntegerUIDPropertyKey(String name) {
        return tx.makeType().name(name).
                vertexUnique(Direction.OUT).
                graphUnique().indexed(Vertex.class).
                dataType(Integer.class).
                makePropertyKey();
    }

    public TitanKey makeWeightPropertyKey(String name) {
        return tx.makeType().name(name).
                vertexUnique(Direction.OUT).
                dataType(Double.class).
                makePropertyKey();
    }

    public TitanKey makeNonUniqueStringPropertyKey(String name) {
        return tx.makeType().name(name).multiValued().
                indexed(Vertex.class).
                dataType(String.class).
                makePropertyKey();
    }

}
