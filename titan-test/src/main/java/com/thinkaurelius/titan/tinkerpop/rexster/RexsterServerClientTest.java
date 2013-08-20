package com.thinkaurelius.titan.tinkerpop.rexster;

import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.TitanGraphTestCommon;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.rexster.client.RexsterClient;
import com.tinkerpop.rexster.client.RexsterClientFactory;
import com.tinkerpop.rexster.server.RexsterSettings;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.XMLConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class RexsterServerClientTest extends TitanGraphTestCommon {

    public final XMLConfiguration rexsterConfig;
    public RexsterTitanServer server;
    public RexsterClient client;

    public RexsterServerClientTest(Configuration config) {
        super(config);
        rexsterConfig = new XMLConfiguration();

        // script engine is not defaulted by rexster.
        rexsterConfig.addProperty("script-engines.script-engine.name", "gremlin-groovy");
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        //Populate graph
        tx.createKeyIndex("name", Vertex.class);
        TitanVertex v1 = tx.addVertex(), v2 = tx.addVertex(), v3 = tx.addVertex();
        v1.setProperty("name", "v1");
        v2.setProperty("name", "v2");
        v3.setProperty("name", "v3");
        tx.addEdge(v1, v2, "knows");
        tx.addEdge(v1, v3, "knows");
        close();
        server = new RexsterTitanServer(rexsterConfig, config);
        server.start();
        client = RexsterClientFactory.open("127.0.0.1", RexsterTitanServer.DEFAULT_GRAPH_NAME);
    }

    @After
    public void tearDown() throws Exception {
        client.close();
        server.stop();
        super.tearDown();
    }

    @Test
    public void simpleQuerying() throws Exception {
        List<Map<String, Object>> result;
//      result = client.query("g.V");
//      assertEquals(3,result.size());
        result = client.execute("g.V('name','v1').out('knows').map");
        assertEquals(2, result.size());
        for (Map<String, Object> map : result) {
            assertTrue(map.containsKey("name"));
            assertTrue(map.get("name").equals("v2") || map.get("name").equals("v3"));
        }
        Map<String, Object> paras = new HashMap<String, Object>();
        paras.put("name", "v1");
        result = client.execute("g.V('name',name).out('knows').map", paras);
        assertEquals(2, result.size());
        for (Map<String, Object> map : result) {
            assertTrue(map.containsKey("name"));
        }
        result = client.execute("g.V('name','v1').out.map");
        assertEquals(2, result.size());
        result = client.execute("g.V('name','v1').outE.map");
        assertEquals(2, result.size());
        try {
            result = client.execute("1/0");
            fail();
        } catch (Exception e) {

        }
    }


}
