package com.thinkaurelius.titan.hadoop.formats.script;

import com.thinkaurelius.titan.hadoop.BaseTest;
import com.thinkaurelius.titan.hadoop.HadoopVertex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.PrintStream;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ScriptRecordWriterTest extends BaseTest {

    public void testRecordWriter() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream stream = new DataOutputStream(new PrintStream(baos));
        Configuration conf = new Configuration();
        conf.setStrings(ScriptOutputFormat.HADOOP_GRAPH_OUTPUT_SCRIPT_FILE, ScriptRecordWriterTest.class.getResource("ScriptOutput.groovy").getFile());
        ScriptRecordWriter writer = new ScriptRecordWriter(stream, conf);
        Map<Long, HadoopVertex> graph = generateGraph(ExampleGraph.TINKERGRAPH);
        for (HadoopVertex vertex : graph.values()) {
            writer.write(NullWritable.get(), vertex);
        }
        String output = baos.toString();
        String[] rows = output.split("\n");
        int vertices = 0;
        for (String row : rows) {
            vertices++;
            assertTrue(row.contains(":"));
            if (row.startsWith("2") || row.startsWith("3") || row.startsWith("5"))
                assertEquals(row.length(), 3);
            else
                assertTrue(row.length() > 3);
        }
        assertEquals(vertices, graph.size());

    }
}
