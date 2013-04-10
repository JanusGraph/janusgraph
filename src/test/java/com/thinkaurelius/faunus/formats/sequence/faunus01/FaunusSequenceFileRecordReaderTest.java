package com.thinkaurelius.faunus.formats.sequence.faunus01;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusSequenceFileRecordReaderTest extends BaseTest {

    public void testRecordReader() throws Exception {
        FaunusSequenceFileRecordReader reader = new FaunusSequenceFileRecordReader();
        reader.initialize(new FileSplit(new Path(FaunusSequenceFileRecordReaderTest.class.getResource("graph-of-the-gods-faunus01.seq").toURI()), 0, Long.MAX_VALUE, new String[]{}),
                new TaskAttemptContext(new Configuration(), new TaskAttemptID()));

        int counter = 0;
        final Map<Long, FaunusVertex> graph = new HashMap<Long, FaunusVertex>();
        while (reader.nextKeyValue()) {
            counter++;
            assertEquals(reader.getCurrentKey(), NullWritable.get());
            graph.put(reader.getCurrentValue().getIdAsLong(), reader.getCurrentValue());
        }
        identicalStructure(graph, BaseTest.ExampleGraph.GRAPH_OF_THE_GODS);
        assertEquals(counter, 12);
        reader.close();
    }
}
