package com.thinkaurelius.titan.hadoop.formats.graphson;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.hadoop.BaseTest;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.VertexQueryFilter;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Direction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphSONRecordReaderTest extends BaseTest {

    public void testRecordReader() throws Exception {
        GraphSONRecordReader reader = new GraphSONRecordReader(VertexQueryFilter.create(new EmptyConfiguration()));
        reader.initialize(new FileSplit(new Path(GraphSONRecordReaderTest.class.getResource("graph-of-the-gods.json").toURI()), 0, Long.MAX_VALUE, new String[]{}),
                HadoopCompatLoader.getCompat().newTask(new Configuration(), new TaskAttemptID()));
        int counter = 0;
        Map<Long, FaunusVertex> graph = new HashMap<Long, FaunusVertex>();
        while (reader.nextKeyValue()) {
            counter++;
            assertEquals(reader.getCurrentKey(), NullWritable.get());
            FaunusVertex vertex = reader.getCurrentValue();
            graph.put(vertex.getLongId(), vertex);
        }
        identicalStructure(graph, ExampleGraph.GRAPH_OF_THE_GODS);
        assertEquals(counter, 12);
        reader.close();
    }

    public void testRecordReaderWithVertexQueryFilterDirection() throws Exception {
        Configuration config = new Configuration();
        ModifiableHadoopConfiguration faunusConf = ModifiableHadoopConfiguration.of(config);
        faunusConf.set(TitanHadoopConfiguration.INPUT_VERTEX_QUERY_FILTER, "v.query().direction(OUT)");
        GraphSONRecordReader reader = new GraphSONRecordReader(VertexQueryFilter.create(config));
        reader.initialize(new FileSplit(new Path(GraphSONRecordReaderTest.class.getResource("graph-of-the-gods.json").toURI()), 0, Long.MAX_VALUE, new String[]{}),
                HadoopCompatLoader.getCompat().newTask(new Configuration(), new TaskAttemptID()));
        int counter = 0;
        while (reader.nextKeyValue()) {
            counter++;
            assertEquals(reader.getCurrentKey(), NullWritable.get());
            FaunusVertex vertex = reader.getCurrentValue();
            assertEquals(Iterables.size(vertex.getEdges(Direction.IN)), 0);
        }
        assertEquals(counter, 12);
        reader.close();
    }

    public void testRecordReaderWithVertexQueryFilterLimit() throws Exception {
        Configuration config = new Configuration();
        ModifiableHadoopConfiguration faunusConf = ModifiableHadoopConfiguration.of(config);
        faunusConf.set(TitanHadoopConfiguration.INPUT_VERTEX_QUERY_FILTER, "v.query().limit(0)");
        GraphSONRecordReader reader = new GraphSONRecordReader(VertexQueryFilter.create(config));
        reader.initialize(new FileSplit(new Path(GraphSONRecordReaderTest.class.getResource("graph-of-the-gods.json").toURI()), 0, Long.MAX_VALUE, new String[]{}),
                HadoopCompatLoader.getCompat().newTask(new Configuration(), new TaskAttemptID()));
        int counter = 0;
        while (reader.nextKeyValue()) {
            counter++;
            assertEquals(reader.getCurrentKey(), NullWritable.get());
            FaunusVertex vertex = reader.getCurrentValue();
            assertEquals(Iterables.size(vertex.getEdges(Direction.BOTH)), 0);
        }
        assertEquals(counter, 12);
        reader.close();
    }
}
