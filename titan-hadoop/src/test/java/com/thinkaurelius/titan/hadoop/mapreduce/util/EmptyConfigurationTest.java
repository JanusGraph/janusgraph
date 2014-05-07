package com.thinkaurelius.titan.hadoop.mapreduce.util;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;

import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EmptyConfigurationTest extends TestCase {

    public void testEmptyConfiguration() {
        Configuration configuration = new Configuration();
        assertTrue(configuration.size() > 1);
        configuration = new EmptyConfiguration();
        assertEquals(configuration.size(), 0);
    }
}
