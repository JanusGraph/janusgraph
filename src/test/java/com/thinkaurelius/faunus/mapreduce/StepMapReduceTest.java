package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StepMapReduceTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, WritableComparable, WritableComparable, WritableComparable, WritableComparable> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, WritableComparable, WritableComparable, WritableComparable, WritableComparable>();
        mapReduceDriver.setMapper(new StepMapReduce.Map());
        mapReduceDriver.setReducer(new StepMapReduce.Reduce());
    }

    public void testAssortativeMixing() throws IOException {

        Configuration config = new Configuration();
        config.setClass(StepMapReduce.CLASS, Vertex.class, Element.class);
        config.set(StepMapReduce.MAP_CLOSURE, "{ v,c -> " +
                "name = new org.apache.hadoop.io.Text(v.name);" +
                "v.outE.each{ c.write(new org.apache.hadoop.io.LongWritable(it.id), name) }; " +
                "v.inE.each{ c.write(new org.apache.hadoop.io.LongWritable(it.id), name) }}");
        config.set(StepMapReduce.REDUCE_CLOSURE, "{ k,v,c -> " +
                "n1 = new org.apache.hadoop.io.Text(v.iterator().next());" +
                "n2 = new org.apache.hadoop.io.Text(v.iterator().next());" +
                "c.write(n1,n2)}");

        mapReduceDriver.withConfiguration(config);

        List<FaunusVertex> vertices = generateGraph(ExampleGraph.GRAPH_OF_THE_GODS);
        for (FaunusVertex v : vertices) {
            v.startPath();
        }
        int counter = 0;
        for (Object object : runWithGraphNoIndex(vertices, mapReduceDriver)) {
            counter++;
            Pair pair = (Pair) object;
            assertNotSame(pair.getFirst(), pair.getSecond());

        }
        assertEquals(counter, 17);

    }
}
