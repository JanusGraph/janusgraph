package org.janusgraph.graphdb.database.management;

import com.google.common.collect.ImmutableSet;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.VertexLabelMaker;
import org.janusgraph.graphdb.JanusGraphBaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public abstract class ManagementTest extends JanusGraphBaseTest {

    private static final Logger log =
            LoggerFactory.getLogger(ManagementTest.class);

    private static final Set<String> ILLEGAL_USER_DEFINED_NAMES = ImmutableSet.of(
            "~key", "~value", "~id", "~nid", "~label", "~adjacent",
            "~timestamp", "~ttl", "~visibility",
            "key", "vertex", "edge", "element", "property", "label",
            "~T$VertexExists",
            "~T$SchemaName",
            "~T$SchemaDefinitionProperty",
            "~T$SchemaCategory",
            "~T$SchemaDefinitionDescription",
            "~T$SchemaUpdateTimestamp",
            "~T$SchemaRelated",
            "~T$VertexLabel");

    @Test
    public void testReservedNamesRejectedForPropertyKeys() {
        for (String s : ILLEGAL_USER_DEFINED_NAMES) {
            JanusGraphManagement tm = graph.openManagement();
            try {
                tm.makePropertyKey(s);
                Assert.fail("Property key  \"" + s + "\" must be rejected");
            } catch (IllegalArgumentException e) {
                log.debug("Caught expected exception", e);
            } finally {
                tm.commit();
            }
        }
    }

    @Test
    public void testReservedNamesRejectedForEdgeLabels() {
        for (String s : ILLEGAL_USER_DEFINED_NAMES) {
            JanusGraphManagement tm = graph.openManagement();
            try {
                tm.makeEdgeLabel(s);
                Assert.fail("Edge label \"" + s + "\" must be rejected");
            } catch (IllegalArgumentException e) {
                log.debug("Caught expected exception", e);
            } finally {
                tm.commit();
            }
        }
    }

    @Test
    public void testReservedNamesRejectedForVertexLabels() {
        for (String s : ILLEGAL_USER_DEFINED_NAMES) {
            JanusGraphManagement tm = graph.openManagement();
            VertexLabelMaker vlm = null;
            try {
                vlm = tm.makeVertexLabel(s);
                Assert.fail("Vertex label \"" + s + "\" must be rejected");
            } catch (IllegalArgumentException e) {
                log.debug("Caught expected exception", e);
            } finally {
                tm.commit();
            }
        }
    }
}
