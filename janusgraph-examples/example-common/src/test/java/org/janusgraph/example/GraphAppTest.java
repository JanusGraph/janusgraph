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

package org.janusgraph.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class GraphAppTest {
    protected static final String CONF_FILE = "conf/jgex-inmemory.properties";

    protected static GraphApp app;
    protected static GraphTraversalSource g;

    @BeforeClass
    public static void setUpClass() throws ConfigurationException {
        app = new GraphApp(CONF_FILE);
        g = app.openGraph();
    }

    @Before
    public void setUp() {
        g.V().drop().iterate();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if (app != null) {
            app.closeGraph();
        }
        app = null;
    }

    @Test
    public void openGraph() throws ConfigurationException {
        assertNotNull(g);
    }

    @Test(expected = ConfigurationException.class)
    public void openGraphNullConfig() throws ConfigurationException {
        new GraphApp(null).openGraph();
    }

    @Test(expected = ConfigurationException.class)
    public void openGraphConfigNotFound() throws ConfigurationException {
        new GraphApp("conf/foobar").openGraph();
    }

    @Test
    public void createElements() throws ConfigurationException {
        app.createElements();

        assertEquals(12L, ((Long) g.V().count().next()).longValue());
        assertEquals(1L, ((Long) g.V().hasLabel("titan").count().next()).longValue());
        assertEquals(1L, ((Long) g.V().hasLabel("demigod").count().next()).longValue());
        assertEquals(1L, ((Long) g.V().hasLabel("human").count().next()).longValue());
        assertEquals(3L, ((Long) g.V().hasLabel("location").count().next()).longValue());
        assertEquals(3L, ((Long) g.V().hasLabel("god").count().next()).longValue());
        assertEquals(3L, ((Long) g.V().hasLabel("monster").count().next()).longValue());

        assertEquals(17L, ((Long) g.E().count().next()).longValue());
        assertEquals(2L, ((Long) g.E().hasLabel("father").count().next()).longValue());
        assertEquals(1L, ((Long) g.E().hasLabel("mother").count().next()).longValue());
        assertEquals(6L, ((Long) g.E().hasLabel("brother").count().next()).longValue());
        assertEquals(1L, ((Long) g.E().hasLabel("pet").count().next()).longValue());
        assertEquals(4L, ((Long) g.E().hasLabel("lives").count().next()).longValue());
        assertEquals(3L, ((Long) g.E().hasLabel("battled").count().next()).longValue());
        final float[] place = (float[]) g.V().has("name", "hercules").outE("battled").has("time", 12).values("place")
                .next();
        assertNotNull(place);
        assertEquals(2, place.length);
        assertEquals(Float.valueOf(39f), Float.valueOf(place[0]));
        assertEquals(Float.valueOf(22f), Float.valueOf(place[1]));
    }

    @Test
    public void updateElements() throws ConfigurationException {
        app.createElements();
        assertFalse(g.V().has("name", "jupiter").has("ts").hasNext());
        app.updateElements();
        final long ts1 = (long) g.V().has("name", "jupiter").values("ts").next();
        app.updateElements();
        final long ts2 = (long) g.V().has("name", "jupiter").values("ts").next();
        assertTrue(ts2 > ts1);
    }

    @Test
    public void deleteElements() {
        app.createElements();
        app.deleteElements();
        assertFalse(g.V().has("name", "pluto").hasNext());
        assertFalse(g.V().has("name", "jupiter").both("brother").has("name", "pluto").hasNext());
    }
}
