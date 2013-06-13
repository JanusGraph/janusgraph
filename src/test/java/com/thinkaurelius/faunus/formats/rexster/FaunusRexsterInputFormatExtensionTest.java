package com.thinkaurelius.faunus.formats.rexster;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraphFactory;
import com.tinkerpop.rexster.extension.ExtensionResponse;
import junit.framework.Assert;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;

import javax.ws.rs.core.Response;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class FaunusRexsterInputFormatExtensionTest {

    @Test
    public void getVertexCountValid() {
        final FaunusRexsterInputFormatExtension ext = new FaunusRexsterInputFormatExtension();
        final Graph g = TinkerGraphFactory.createTinkerGraph();

        final ExtensionResponse response = ext.getVertexCount(g);
        Assert.assertNotNull(response);

        final Response r = response.getJerseyResponse();
        Assert.assertEquals(200, r.getStatus());

        final JSONObject json = (JSONObject) r.getEntity();
        Assert.assertNotNull(json);
        Assert.assertEquals(6, json.optInt("count"));
    }
}
