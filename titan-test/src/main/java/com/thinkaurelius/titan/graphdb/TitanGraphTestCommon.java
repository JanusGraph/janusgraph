package com.thinkaurelius.titan.graphdb;


import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Decimal;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;

public abstract class TitanGraphTestCommon extends TitanGraphBaseTest {

    public static final int DEFAULT_THREAD_COUNT = 4;

    public static int getThreadCount() {
        String s = System.getProperty("titan.test.threads");
        if (null != s)
            return Integer.valueOf(s);
        else
            return DEFAULT_THREAD_COUNT;
    }

    public static int wrapAround(int value, int maxValue) {
        value = value % maxValue;
        if (value < 0) value = value + maxValue;
        return value;
    }

    public TitanLabel makeSimpleEdgeLabel(String name) {
        return tx.makeLabel(name).make();
    }

    public TitanLabel makeKeyedEdgeLabel(String name, TitanKey sort, TitanKey signature) {
        TitanLabel relType = tx.makeLabel(name).
                sortKey(sort).signature(signature).directed().make();
        return relType;
    }

    public TitanKey makeUniqueStringPropertyKey(String name) {
        return tx.makeKey(name).single().unique().indexed(Vertex.class).dataType(String.class).make();
    }

    public TitanKey makeStringUIDPropertyKey(String name) {
        return tx.makeKey(name).
                single().unique().indexed(Vertex.class).
                dataType(String.class).
                make();
    }

    public TitanKey makeStringPropertyKey(String name) {
        return tx.makeKey(name).single().
                indexed(Vertex.class).
                dataType(String.class).
                make();
    }

    public TitanKey makeUnindexedStringPropertyKey(String name) {
        return tx.makeKey(name).single().
                dataType(String.class).
                make();
    }

    public TitanKey makeIntegerUIDPropertyKey(String name) {
        return tx.makeKey(name).single().unique().indexed(Vertex.class).
                dataType(Integer.class).
                make();
    }

    public TitanKey makeWeightPropertyKey(String name) {
        return tx.makeKey(name).single().
                dataType(Decimal.class).
                make();
    }


}
