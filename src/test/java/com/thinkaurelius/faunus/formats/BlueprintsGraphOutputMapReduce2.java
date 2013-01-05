package com.thinkaurelius.faunus.formats;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BlueprintsGraphOutputMapReduce2 extends BlueprintsGraphOutputMapReduce {

    private static Graph graph = new TinkerGraph();

    public static Graph getGraph() {
        return graph;
    }

    public static class Map2 extends BlueprintsGraphOutputMapReduce.Map {
        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.graph = BlueprintsGraphOutputMapReduce2.getGraph();
        }
    }

    public static class Reduce2 extends BlueprintsGraphOutputMapReduce.Reduce {
        @Override
        public void setup(final Reduce.Context context) throws IOException, InterruptedException {
            this.graph = BlueprintsGraphOutputMapReduce2.getGraph();
        }
    }
}
