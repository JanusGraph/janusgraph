package com.thinkaurelius.faunus.formats.script;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ScriptRecordReaderTest extends BaseTest {

    public void testRecordReader() throws Exception {
        final Configuration conf = new Configuration();
        conf.setStrings(ScriptInputFormat.INPUT_SCRIPT_FILE, ScriptRecordReaderTest.class.getResource("ScriptInput.groovy").getFile());
        ScriptRecordReader reader = new ScriptRecordReader(new TaskAttemptContext(conf, new TaskAttemptID()));
        reader.initialize(new FileSplit(new Path(ScriptRecordReaderTest.class.getResource("graph-of-the-gods.id").toURI()), 0, Long.MAX_VALUE, new String[]{}),
                new TaskAttemptContext(conf, new TaskAttemptID()));
        int counter = 0;
        while (reader.nextKeyValue()) {
            assertEquals(reader.getCurrentKey(), NullWritable.get());
            FaunusVertex vertex = reader.getCurrentValue();
            long id = vertex.getIdAsLong();
            assertEquals(id, counter++);
            assertEquals(vertex.getPropertyKeys().size(), 0);
            assertEquals(count(vertex.getEdges(Direction.IN)), 0);
            if (id == 1 || id == 2 || id == 3 || id == 7 || id == 11) {
                assertTrue(count(vertex.getEdges(Direction.OUT)) > 0);
            } else {
                assertTrue(count(vertex.getEdges(Direction.OUT)) == 0);
            }
        }
        assertEquals(counter, 12);
        reader.close();
    }
}
