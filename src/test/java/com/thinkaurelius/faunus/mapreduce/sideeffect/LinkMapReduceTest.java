package com.thinkaurelius.faunus.mapreduce.sideeffect;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LinkMapReduceTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new LinkMapReduce.Map());
        mapReduceDriver.setReducer(new LinkMapReduce.Reduce());
    }

    public void testKnowsCreatedTraversal() throws IOException {

        Configuration config = new Configuration();
        config.set(LinkMapReduce.TAG, "a");
        config.set(LinkMapReduce.DIRECTION, Direction.OUT.name());
        config.set(LinkMapReduce.LABEL, "knowsCreated");

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = generateIndexedToyGraph(BaseTest.ExampleGraph.TINKERGRAPH);
        for (FaunusVertex vertex : results.values()) {
            vertex.removeEdges(Tokens.Action.DROP, Direction.BOTH);
        }
        results.get(1l).setTag('a');
        results.get(3l).setEnergy(1);
        results.get(5l).setEnergy(1);

        results = runWithGraph(results.values(), mapReduceDriver);
        for (FaunusVertex vertex : results.values()) {
            System.out.println(vertex.getEnergy() + "-" + vertex.getTag());
            System.out.println(BaseTest.getFullString(vertex));
        }
    }


}
