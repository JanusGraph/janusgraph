package com.thinkaurelius.faunus.formats.sequence.faunus01;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusSequenceFileRecordReaderTest extends BaseTest {

    public void testConversionWithReader() throws Exception {
        final Configuration conf = new Configuration();
        final SequenceFile.Reader reader = new SequenceFile.Reader(
                FileSystem.get(conf),
                new Path(FaunusVertex01Test.class.getResource("graph-of-the-gods-faunus01.seq").toURI()),
                conf);
        FaunusSequenceFileRecordReader.prepareSerializationReader(reader, conf);

        final NullWritable key = NullWritable.get();
        final FaunusVertex01 value = new FaunusVertex01();

        final Map<Long, FaunusVertex> graph = new HashMap<Long, FaunusVertex>();
        while (reader.next(key, value)) {
            graph.put(value.getIdAsLong(), VertexConverter.buildFaunusVertex(value));
        }
        identicalStructure(graph, BaseTest.ExampleGraph.GRAPH_OF_THE_GODS);
        reader.close();
    }
}
