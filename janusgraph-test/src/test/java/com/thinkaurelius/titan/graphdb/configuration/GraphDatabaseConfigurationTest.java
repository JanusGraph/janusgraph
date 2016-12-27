package com.thinkaurelius.titan.graphdb.configuration;

import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class GraphDatabaseConfigurationTest {

    @Test
    public void testUniqueNames() {
        assertFalse(StringUtils.containsAny(GraphDatabaseConfiguration.getOrGenerateUniqueInstanceId(Configuration.EMPTY), ConfigElement.ILLEGAL_CHARS));
    }


}
