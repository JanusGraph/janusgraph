package com.thinkaurelius.faunus.mapreduce.algebra;

import com.thinkaurelius.faunus.io.graph.FaunusEdge;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeFunctionFilter {

    public static final String FUNCTION = Tokens.makeNamespace(EdgeFunctionFilter.class) + ".function";

    public enum Counters {
        EDGES_ALLOWED,
        EDGES_FILTERED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private Function<FaunusEdge, Boolean> function;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            Class c = null;
            try {
                c = context.getConfiguration().getClass(FUNCTION, null);
                this.function = (Function<FaunusEdge, Boolean>) c.getConstructor().newInstance();
            } catch (Exception e) {
                throw new IOException("Unable to construct function: " + c);
            }
        }


        @Override
        public void map(final NullWritable key, final FaunusVertex value, final org.apache.hadoop.mapreduce.Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            long filterCounter = 0;
            long allowedCounter = 0;

            List<Edge> newEdges = new ArrayList<Edge>();
            for (final Edge edge : value.getEdges(OUT)) {
                if (this.function.compute((FaunusEdge) edge)) {
                    newEdges.add(edge);
                    allowedCounter++;
                } else {
                    filterCounter++;
                }
            }
            value.setEdges(OUT, newEdges);

            newEdges = new ArrayList<Edge>();
            for (final Edge edge : value.getEdges(IN)) {
                if (this.function.compute((FaunusEdge) edge)) {
                    newEdges.add(edge);
                    allowedCounter++;
                } else {
                    filterCounter++;
                }
            }
            value.setEdges(IN, newEdges);

            context.write(NullWritable.get(), value);
            context.getCounter(Counters.EDGES_ALLOWED).increment(allowedCounter);
            context.getCounter(Counters.EDGES_FILTERED).increment(filterCounter);
        }
    }
}
