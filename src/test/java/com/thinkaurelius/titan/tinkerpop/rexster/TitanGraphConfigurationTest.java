package com.thinkaurelius.titan.tinkerpop.rexster;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.rexster.Tokens;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Matthias Broecheler (http://www.matthiasb.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */

public class TitanGraphConfigurationTest {
    
    final TitanGraphConfiguration rexsterConfig = new TitanGraphConfiguration(); 
    
    @Test
    public void testGraphConfiguration() throws Exception {
        XMLConfiguration configuration = new XMLConfiguration(ClassLoader.getSystemResource("rexster-fragment.xml"));
        
        Configuration tc = rexsterConfig.convertConfiguration(configuration);
        Configuration storage = tc.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        assertEquals(storage.getBoolean(GraphDatabaseConfiguration.STORAGE_READONLY_KEY), false);
        assertEquals(storage.getString(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY), "home");
        assertEquals(storage.getString(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY), "local");
    }
    
    private static final String subProperty(String key) {
        return Tokens.REXSTER_GRAPH_PROPERTIES + "." + key;
    }
    
}
