package org.janusgraph.graphdb.inmemory;

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class InMemoryConfigurationTest {

    StandardJanusGraph graph;

    public void initialize(ConfigOption option, Object value) {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"inmemory");
        config.set(option,value);
        graph = (StandardJanusGraph) JanusGraphFactory.open(config);
    }

    @After
    public void shutdown() {
        graph.close();
    }


    @Test
    public void testReadOnly() {
        initialize(GraphDatabaseConfiguration.STORAGE_READONLY,true);

        JanusGraphTransaction tx = graph.newTransaction();
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
