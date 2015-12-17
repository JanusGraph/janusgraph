package com.thinkaurelius.titan.graphdb;

import com.thinkaurelius.titan.example.GraphOfTheGodsFactory;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * Tests Titan specific serialization classes not covered by the TinkerPop suite.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class TitanIoTest extends TitanGraphBaseTest {

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

        TitanIndexTest.assertGraphOfTheGods(graph);
    }

    @Test
    public void testGeoShapeSerializationReadWriteAsGryo() throws Exception {
        GraphOfTheGodsFactory.loadWithoutMixedIndex(graph, true);
        graph.io(IoCore.gryo()).writeGraph("/tmp/test.kryo");

        clearGraph(config);
        open(config);

        graph.io(IoCore.gryo()).readGraph("/tmp/test.kryo");

        TitanIndexTest.assertGraphOfTheGods(graph);
    }
}
