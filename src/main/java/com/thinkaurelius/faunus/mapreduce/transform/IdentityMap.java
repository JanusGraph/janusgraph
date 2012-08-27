package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityMap {

    public enum Counters {
        VERTEX_COUNT,
        EDGE_COUNT
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), value);
            context.getCounter(Counters.VERTEX_COUNT).increment(1l);
            context.getCounter(Counters.EDGE_COUNT).increment(((List) value.getEdges(Direction.BOTH)).size());
        }
    }
}
