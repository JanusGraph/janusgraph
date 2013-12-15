package com.thinkaurelius.faunus.formats;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusGraph;
import org.apache.hadoop.conf.Configuration;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanOutputFormatTest extends BaseTest {

    public void testTrue() {
        assertTrue(true);
    }

    public FaunusGraph generateFaunusGraph(final InputStream inputStream) throws Exception {
        Configuration configuration = new Configuration();
        Properties properties = new Properties();
        properties.load(inputStream);
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            configuration.set(entry.getKey().toString(), entry.getValue().toString());
        }
        return new FaunusGraph(configuration);
    }
}
