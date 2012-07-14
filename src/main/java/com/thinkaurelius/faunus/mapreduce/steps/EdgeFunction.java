package com.thinkaurelius.faunus.mapreduce.steps;

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
public class EdgeFunction {

    public static final String FUNCTION = Tokens.makeNamespace(EdgeFunction.class) + ".function";

    public enum Counters {
        VERTICES_PROCESSED,
        EDGES_PROCESSED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private Function<FaunusEdge, FaunusEdge> function;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            Class c = null;
            try {
                c = context.getConfiguration().getClass(FUNCTION, null);
                this.function = (Function<FaunusEdge, FaunusEdge>) c.getConstructor().newInstance();
            } catch (Exception e) {
                throw new IOException("Unable to construct function: " + c);
            }
        }


        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            long edgeCounter = 0;
            for (final Edge edge : value.getEdges(Direction.BOTH)) {
                function.compute((FaunusEdge) edge);
                edgeCounter++;

            }
            context.getCounter(Counters.VERTICES_PROCESSED).increment(1);
            context.getCounter(Counters.EDGES_PROCESSED).increment(edgeCounter);
            context.write(NullWritable.get(), value);
        }
    }
}
