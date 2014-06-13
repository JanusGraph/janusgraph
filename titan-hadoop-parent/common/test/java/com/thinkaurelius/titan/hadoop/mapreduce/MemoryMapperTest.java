package com.thinkaurelius.titan.hadoop.mapreduce;

import com.thinkaurelius.titan.hadoop.BaseTest;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MemoryMapperTest extends BaseTest {

    public void testGlobalCurrentConfiguration() {
        assertTrue("asdf-113".matches(".*-[0-9]+"));
        assertFalse("asdf-".matches(".*-[0-9]+"));
        assertFalse("asd-34f".matches(".*-[0-9]+"));
        assertTrue("ads-ff-123".matches(".*-[0-9]+"));
    }
}
