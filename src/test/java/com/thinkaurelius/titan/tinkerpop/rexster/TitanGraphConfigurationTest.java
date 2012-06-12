package com.thinkaurelius.titan.tinkerpop.rexster;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.rexster.Tokens;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class TitanGraphConfigurationTest {
    
    final TitanGraphConfiguration rexsterConfig = new TitanGraphConfiguration(); 
    
    @Test
    public void testGraphConfiguration() throws Exception {
        HierarchicalConfiguration rc = new HierarchicalConfiguration();
        rc.addProperty(Tokens.REXSTER_GRAPH_LOCATION, StorageSetup.getHomeDir());
        rc.addProperty(Tokens.REXSTER_GRAPH_READ_ONLY, false);
        rc.addProperty(subProperty("storage.backend"),"local");
        
//        Configuration cc = rc.configurationAt("rexster");
//        assertEquals(cc.getString("one"),"two");
        
        Configuration tc = rexsterConfig.convertConfiguration(rc);
        Configuration storage = tc.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        assertEquals(storage.getString(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY),"local");
        assertEquals(storage.getBoolean(GraphDatabaseConfiguration.STORAGE_READONLY_KEY),false);
        assertEquals(storage.getString(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY),StorageSetup.getHomeDir());
        
        TitanGraph graph = (TitanGraph)rexsterConfig.configureGraphInstance(rc);
        assertTrue(graph.isOpen());
        graph.shutdown();
    }
    
    private static final String subProperty(String key) {
        return Tokens.REXSTER_GRAPH_PROPERTIES + "." + key;
    }
    
}
