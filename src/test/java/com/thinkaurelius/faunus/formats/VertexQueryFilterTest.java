package com.thinkaurelius.faunus.formats;

import com.tinkerpop.blueprints.Direction;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexQueryFilterTest extends TestCase {

    public void testDegenerateVertexQuery() {
        Configuration config = new Configuration();
        VertexQueryFilter query = VertexQueryFilter.create(config);
        assertFalse(query.doesFilter());
        assertEquals(query.limit, Long.MAX_VALUE);
        assertEquals(query.hasContainers.size(), 0);
        assertEquals(query.direction, Direction.BOTH);
        assertEquals(query.labels.length, 0);
    }

    public void testVertexQuery() {
        Configuration config = new Configuration();
        config.set(VertexQueryFilter.FAUNUS_GRAPH_INPUT_VERTEX_QUERY_FILTER, "v.query().limit(0)");
        VertexQueryFilter query = VertexQueryFilter.create(config);
        assertTrue(query.doesFilter());
        assertEquals(query.limit, 0);
        assertEquals(query.hasContainers.size(), 0);
        assertEquals(query.direction, Direction.BOTH);
        assertEquals(query.labels.length, 0);
    }
}
