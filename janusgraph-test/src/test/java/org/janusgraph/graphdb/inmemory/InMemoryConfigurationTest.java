package org.janusgraph.graphdb.inmemory;

import org.janusgraph.core.TitanFactory;
import org.janusgraph.core.TitanTransaction;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardTitanGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class InMemoryConfigurationTest {

    StandardTitanGraph graph;

    public void initialize(ConfigOption option, Object value) {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"inmemory");
        config.set(option,value);
        graph = (StandardTitanGraph) TitanFactory.open(config);
    }

    @After
    public void shutdown() {
        graph.close();
    }


    @Test
    public void testReadOnly() {
        initialize(GraphDatabaseConfiguration.STORAGE_READONLY,true);

        TitanTransaction tx = graph.newTransaction();
        try {
            tx.addVertex();
            fail();
        } catch (Exception e ) {
        } finally {
            tx.rollback();
        }

        try {
            graph.addVertex();
            fail();
        } catch (Exception e ) {
        } finally {
            graph.tx().rollback();
        }

    }


}
