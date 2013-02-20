package com.thinkaurelius.titan.graphdb;


import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.core.TypeMaker;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;

public abstract class TitanGraphTestCommon {

    public Configuration config;
    public TitanGraph graphdb;
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
        graphdb = TitanFactory.open(config);
        tx = graphdb.newTransaction();
    }

    public void close() {
        if (null != tx && tx.isOpen())
            tx.commit();

        if (null != graphdb)
            graphdb.shutdown();
    }

    public void newTx() {
        if (null != tx && tx.isOpen())
            tx.commit();
        tx = graphdb.newTransaction();
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
        return makeSimpleEdgeLabel(name, TypeGroup.DEFAULT_GROUP);
    }

    public TitanLabel makeSimpleEdgeLabel(String name, TypeGroup group) {
        return makeSimpleEdgeLabel(name, group, Directionality.Directed);
    }

    public TitanLabel makeSimpleEdgeLabel(String name, TypeGroup group, Directionality dir) {
        TypeMaker etmaker = tx.makeType();
        etmaker.name(name).simple().group(group);
        switch (dir) {
            case Undirected:
                etmaker.undirected();
                break;
            case Unidirected:
                etmaker.unidirected();
                break;
            case Directed:
                etmaker.directed();
                break;
        }
        return etmaker.makeEdgeLabel();
    }

    public TitanLabel makeKeyedEdgeLabel(String name, TitanKey primary, TitanKey signature) {
        TypeMaker etmaker = tx.makeType();
        TitanLabel relType = etmaker.name(name).
                primaryKey(primary).signature(signature).directed().makeEdgeLabel();
        return relType;
    }

    public TitanKey makeUniqueStringPropertyKey(String name) {
        return tx.makeType().name(name).simple().functional().
                unique().indexed().dataType(String.class).makePropertyKey();
    }

    public TitanKey makeStringUIDPropertyKey(String name, TypeGroup group) {
        return tx.makeType().name(name).
                simple().functional().
                unique().indexed().
                dataType(String.class).group(group).
                makePropertyKey();
    }

    public TitanKey makeStringPropertyKey(String name) {
        return tx.makeType().name(name).
                simple().indexed().
                dataType(String.class).
                makePropertyKey();
    }


    public TitanKey makeIntegerUIDPropertyKey(String name) {
        return makeIntegerUIDPropertyKey(name, TypeGroup.DEFAULT_GROUP);
    }

    public TitanKey makeIntegerUIDPropertyKey(String name, TypeGroup group) {
        return tx.makeType().name(name).
                simple().functional().
                unique().indexed().
                dataType(Integer.class).group(group).
                makePropertyKey();
    }

    public TitanKey makeWeightPropertyKey(String name) {
        return tx.makeType().name(name).
                simple().functional().
                dataType(Double.class).
                makePropertyKey();
    }

}
