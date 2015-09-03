package com.thinkaurelius.titan.graphdb.configuration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class TinkerpopFeaturesTest
{

    private TitanGraph graph;

    @Before
    public void setupGraph()
    {
        graph = open(false);
    }

    @After
    public void closeGraph()
    {
        if (null != graph)
            graph.close();
    }

    public TitanGraph open(boolean useVertexIdSetting)
    {
        TitanFactory.Builder builder = TitanFactory.build();
        builder.set("storage.backend", "inmemory");
        builder.set("graph.set-vertex-id", useVertexIdSetting);
        return builder.open();
    }

    @Test
    public void testVertexFeatures()
    {
        Graph.Features.VertexFeatures vf = graph.features().vertex();
        assertFalse(vf.supportsCustomIds());
        assertFalse(vf.supportsStringIds());
        assertFalse(vf.supportsUuidIds());
        assertFalse(vf.supportsAnyIds());
        assertFalse(vf.supportsCustomIds());
        assertTrue(vf.supportsNumericIds());
        assertFalse(vf.supportsUserSuppliedIds());

        graph.close();
        graph = open(true);

        vf = graph.features().vertex();

        assertFalse(vf.supportsCustomIds());
        assertFalse(vf.supportsStringIds());
        assertFalse(vf.supportsUuidIds());
        assertFalse(vf.supportsAnyIds());
        assertFalse(vf.supportsCustomIds());
        assertTrue(vf.supportsNumericIds());
        assertTrue(vf.supportsUserSuppliedIds());
    }

    @Test
    public void testEdgeFeatures()
    {
        Graph.Features.EdgeFeatures ef = graph.features().edge();
        assertFalse(ef.supportsStringIds());
        assertFalse(ef.supportsUuidIds());
        assertFalse(ef.supportsAnyIds());
        assertFalse(ef.supportsNumericIds());
        assertTrue(ef.supportsCustomIds());
        assertFalse(ef.supportsUserSuppliedIds());
    }
}
