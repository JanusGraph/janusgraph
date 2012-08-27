package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VerticesMap {

    public enum Counters {
        VERTICES_PROCESSED,
        EDGES_PROCESSED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            value.startPath();
            long edgesProcessed = 0;
            for (final Edge edge : value.getEdges(Direction.BOTH)) {
                ((FaunusEdge) edge).clearPaths();
                edgesProcessed++;
            }
            context.write(NullWritable.get(), value);
            context.getCounter(Counters.VERTICES_PROCESSED).increment(1l);
            context.getCounter(Counters.EDGES_PROCESSED).increment(edgesProcessed);
        }
    }
}
