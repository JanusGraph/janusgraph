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

package org.janusgraph.graphdb;

import org.janusgraph.example.GraphOfTheGodsFactory;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * Tests JanusGraph specific serialization classes not covered by the TinkerPop suite.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class JanusGraphIoTest extends JanusGraphBaseTest {

    @Test
    public void testGeoShapeSerializationReadWriteAsGraphSONEmbedded() throws Exception {
        GraphOfTheGodsFactory.loadWithoutMixedIndex(graph, true);
        GraphSONMapper m = graph.io(IoCore.graphson()).mapper().embedTypes(true).create();
        GraphWriter writer = graph.io(IoCore.graphson()).writer().mapper(m).create();
        FileOutputStream fos = new FileOutputStream("/tmp/test.json");
        writer.writeGraph(fos, graph);

        clearGraph(config);
        open(config);

        GraphReader reader = graph.io(IoCore.graphson()).reader().mapper(m).create();
        FileInputStream fis = new FileInputStream("/tmp/test.json");
        reader.readGraph(fis, graph);

        JanusGraphIndexTest.assertGraphOfTheGods(graph);
    }

    @Test
    public void testGeoShapeSerializationReadWriteAsGryo() throws Exception {
        GraphOfTheGodsFactory.loadWithoutMixedIndex(graph, true);
        graph.io(IoCore.gryo()).writeGraph("/tmp/test.kryo");

        clearGraph(config);
        open(config);

        graph.io(IoCore.gryo()).readGraph("/tmp/test.kryo");

        JanusGraphIndexTest.assertGraphOfTheGods(graph);
    }
}
