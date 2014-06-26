package com.thinkaurelius.titan.hadoop.formats.titan;

import com.thinkaurelius.titan.hadoop.BaseTest;
import com.thinkaurelius.titan.hadoop.formats.titan.util.ConfigurationUtil;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphFactoryTest extends BaseTest {

    public void testPropertyPrefixes() {
        Configuration configuration = new EmptyConfiguration();
        configuration.set("titan.hadoop.output.storage.backend", "cassandrathrift");
        configuration.set("titan.hadoop.output.ids.block-size", "100000");
        configuration.set("titan.hadoop.output.storage.batch-loading", "true");
        BaseConfiguration base = ConfigurationUtil.extractConfiguration(configuration, TitanOutputFormat.TITAN_HADOOP_GRAPH_OUTPUT_TITAN);
        assertEquals(base.getString("storage.backend"), "cassandrathrift");
        assertEquals(base.getLong("ids.block-size"), 100000);
        assertTrue(base.getBoolean("storage.batch-loading"));
        assertEquals(base.getString("titan.hadoop.output.storage.backend"), "cassandrathrift");
        assertEquals(base.getLong(  "titan.hadoop.output.ids.block-size"), 100000);
        assertTrue(base.getBoolean( "titan.hadoop.output.storage.batch-loading"));
        assertEquals(count(base.getKeys()), 6);
    }
}
