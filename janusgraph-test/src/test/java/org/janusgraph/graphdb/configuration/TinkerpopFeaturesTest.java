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

package org.janusgraph.graphdb.configuration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class TinkerpopFeaturesTest
{

    private JanusGraph graph;

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

    public JanusGraph open(boolean useVertexIdSetting)
    {
        JanusGraphFactory.Builder builder = JanusGraphFactory.build();
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
