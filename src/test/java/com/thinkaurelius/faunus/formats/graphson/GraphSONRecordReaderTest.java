package com.thinkaurelius.faunus.formats.graphson;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphSONRecordReaderTest extends BaseTest {

    public void testRecordReader() throws Exception {
        GraphSONRecordReader reader = new GraphSONRecordReader();
        reader.initialize(new FileSplit(new Path(GraphSONRecordReaderTest.class.getResource("graph-of-the-gods.json").toURI()), 0, Long.MAX_VALUE, new String[]{}),
                new TaskAttemptContext(new Configuration(), new TaskAttemptID()));
        int counter = 0;
        Map<Long, FaunusVertex> graph = new HashMap<Long, FaunusVertex>();
        while (reader.nextKeyValue()) {
            counter++;
            assertEquals(reader.getCurrentKey(), NullWritable.get());
            FaunusVertex vertex = reader.getCurrentValue();
            graph.put(vertex.getIdAsLong(), vertex);
        }
        identicalStructure(graph, ExampleGraph.GRAPH_OF_THE_GODS);
        assertEquals(counter, 12);
        reader.close();
    }
}
