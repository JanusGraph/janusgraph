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

package org.janusgraph.graphdb.berkeleyje;

import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.util.JanusGraphCleanup;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import org.janusgraph.BerkeleyStorageSetup;
import org.janusgraph.diskstorage.berkeleyje.BerkeleyJEStoreManager;
import org.janusgraph.diskstorage.berkeleyje.BerkeleyJEStoreManager.IsolationLevel;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphTest;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import static org.junit.Assert.*;

public class BerkeleyGraphTest extends JanusGraphTest {

    @Rule
    public TestName methodNameRule = new TestName();

    private static final Logger log =
            LoggerFactory.getLogger(BerkeleyGraphTest.class);

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration mcfg = BerkeleyStorageSetup.getBerkeleyJEConfiguration();
        String methodName = methodNameRule.getMethodName();
        if (methodName.equals("testConsistencyEnforcement")) {
            IsolationLevel iso = IsolationLevel.SERIALIZABLE;
            log.debug("Forcing isolation level {} for test method {}", iso, methodName);
            mcfg.set(BerkeleyJEStoreManager.ISOLATION_LEVEL, iso.toString());
        } else {
            IsolationLevel iso = null;
            if (mcfg.has(BerkeleyJEStoreManager.ISOLATION_LEVEL)) {
                iso = ConfigOption.getEnumValue(mcfg.get(BerkeleyJEStoreManager.ISOLATION_LEVEL),IsolationLevel.class);
            }
            log.debug("Using isolation level {} (null means adapter default) for test method {}", iso, methodName);
        }
        return mcfg.getConfiguration();
    }

    @Test
    public void testVertexCentricQuerySmall() {
        testVertexCentricQuery(1450 /*noVertices*/);
    }

    @Override
    public void testConsistencyEnforcement() {
        // Check that getConfiguration() explicitly set serializable isolation
        // This could be enforced with a JUnit assertion instead of a Precondition,
        // but a failure here indicates a problem in the test itself rather than the
        // system-under-test, so a Precondition seems more appropriate
        IsolationLevel effective = ConfigOption.getEnumValue(config.get(ConfigElement.getPath(BerkeleyJEStoreManager.ISOLATION_LEVEL), String.class),IsolationLevel.class);
        Preconditions.checkState(IsolationLevel.SERIALIZABLE.equals(effective));
        super.testConsistencyEnforcement();
    }

    @Override
    public void testConcurrentConsistencyEnforcement() {
        //Do nothing TODO: Figure out why this is failing in BerkeleyDB!!
    }

    @Test
    public void testIDBlockAllocationTimeout()
    {
        config.set("ids.authority.wait-time", Duration.of(0L, ChronoUnit.NANOS));
        config.set("ids.renew-timeout", Duration.of(1L, ChronoUnit.MILLIS));
        close();
        JanusGraphCleanup.clear(graph);
        open(config);
        try {
            graph.addVertex();
            fail();
        } catch (JanusGraphException e) {

        }

        assertTrue(graph.isOpen());

        close(); // must be able to close cleanly

        // Must be able to reopen
        open(config);

        assertEquals(0L, (long)graph.traversal().V().count().next());
    }
}
