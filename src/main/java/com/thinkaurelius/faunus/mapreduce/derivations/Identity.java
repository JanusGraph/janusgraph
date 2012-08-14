package com.thinkaurelius.faunus.mapreduce.derivations;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

import static com.tinkerpop.blueprints.Direction.BOTH;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Identity {

    public enum Counters {
        VERTEX_COUNT,
        EDGE_COUNT
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {
        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            context.getCounter(Counters.VERTEX_COUNT).increment(1);
            context.getCounter(Counters.EDGE_COUNT).increment(((List) value.getEdges(BOTH)).size());
            context.write(NullWritable.get(), value);
        }
    }
}
