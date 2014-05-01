package com.thinkaurelius.titan.hadoop.formats.titan;

import com.thinkaurelius.titan.hadoop.BaseTest;
import com.thinkaurelius.titan.hadoop.formats.titan.TitanOutputFormat;
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
        configuration.set("faunus.graph.output.titan.storage.backend", "cassandrathrift");
        configuration.set("faunus.graph.output.titan.ids.block-size", "100000");
        configuration.set("faunus.graph.output.titan.storage.batch-loading", "true");
        BaseConfiguration base = ConfigurationUtil.extractConfiguration(configuration, TitanOutputFormat.FAUNUS_GRAPH_OUTPUT_TITAN);
        assertEquals(base.getString("storage.backend"), "cassandrathrift");
        assertEquals(base.getLong("ids.block-size"), 100000);
        assertTrue(base.getBoolean("storage.batch-loading"));
        assertEquals(base.getString("faunus.graph.output.titan.storage.backend"), "cassandrathrift");
        assertEquals(base.getLong("faunus.graph.output.titan.ids.block-size"), 100000);
        assertTrue(base.getBoolean("faunus.graph.output.titan.storage.batch-loading"));
        assertEquals(count(base.getKeys()), 6);
    }
}
