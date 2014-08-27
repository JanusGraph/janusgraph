package com.thinkaurelius.titan.hadoop.formats.script;

import com.thinkaurelius.titan.hadoop.BaseTest;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.VertexQueryFilter;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Direction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import static com.thinkaurelius.titan.hadoop.formats.script.ScriptConfig.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ScriptRecordReaderTest extends BaseTest {

    public void testRecordReader() throws Exception {
        final Configuration conf = new Configuration();
        ModifiableHadoopConfiguration faunusConf = ModifiableHadoopConfiguration.of(conf);
        faunusConf.getInputConf(ROOT_NS).set(SCRIPT_FILE, ScriptRecordReaderTest.class.getResource("ScriptInput.groovy").getFile());
        ScriptRecordReader reader = new ScriptRecordReader(VertexQueryFilter.create(new EmptyConfiguration()), HadoopCompatLoader.getCompat().newTask(conf, new TaskAttemptID()));
        reader.initialize(new FileSplit(new Path(ScriptRecordReaderTest.class.getResource("graph-of-the-gods.id").toURI()), 0, Long.MAX_VALUE, new String[]{}),
                HadoopCompatLoader.getCompat().newTask(conf, new TaskAttemptID()));
        int counter = 0;
        while (reader.nextKeyValue()) {
            assertEquals(reader.getCurrentKey(), NullWritable.get());
            FaunusVertex vertex = reader.getCurrentValue();
            long id = vertex.getLongId();
            assertEquals(id, counter++);
            assertEquals(vertex.getPropertyKeys().size(), 0);
            assertEquals(count(vertex.getEdges(Direction.IN)), 0);
            if (id == 1 || id == 2 || id == 3 || id == 7 || id == 11) {
                assertTrue(count(vertex.getEdges(Direction.OUT)) > 0);
            } else {
                assertEquals(count(vertex.getEdges(Direction.OUT)), 0);
            }
        }
        assertEquals(counter, 12);
        reader.close();
    }
}
