package com.thinkaurelius.faunus.mapreduce.derivations;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

import static com.tinkerpop.blueprints.Direction.BOTH;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DirectionFilter {
    public static final String DIRECTION = Tokens.makeNamespace(DirectionFilter.class) + ".direction";
    public static final String ACTION = Tokens.makeNamespace(DirectionFilter.class) + ".action";

    public enum Counters {
        EDGES_KEPT,
        EDGES_DROPPED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private Direction direction;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.direction = Direction.valueOf(context.getConfiguration().get(DIRECTION));
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            long originalSize = ((List) value.getEdges(BOTH)).size();
            value.removeEdges(Tokens.Action.DROP, this.direction);
            long newSize = ((List) value.getEdges(BOTH)).size();

            context.getCounter(Counters.EDGES_KEPT).increment(originalSize - (originalSize - newSize));
            context.getCounter(Counters.EDGES_DROPPED).increment(originalSize - newSize);

            context.write(NullWritable.get(), value);

        }
    }
}
