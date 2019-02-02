// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.database.management;

import com.google.common.collect.ImmutableSet;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.JanusGraphBaseTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.fail;

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
                fail("Property key  \"" + s + "\" must be rejected");
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
                fail("Edge label \"" + s + "\" must be rejected");
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
            try {
                tm.makeVertexLabel(s);
                fail("Vertex label \"" + s + "\" must be rejected");
            } catch (IllegalArgumentException e) {
                log.debug("Caught expected exception", e);
            } finally {
                tm.commit();
            }
        }
    }
}
