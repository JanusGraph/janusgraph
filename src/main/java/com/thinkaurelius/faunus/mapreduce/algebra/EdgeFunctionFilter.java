package com.thinkaurelius.faunus.mapreduce.algebra;

import com.thinkaurelius.faunus.io.graph.FaunusEdge;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.algebra.util.Counters;
import com.thinkaurelius.faunus.mapreduce.algebra.util.Function;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeFunctionFilter {

    public static final String FUNCTION = "faunus.algebra.edgefunctionfilter.function";

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

            final List<Edge> newEdges = new ArrayList<Edge>();
            for (final Edge edge : value.getEdges(Direction.OUT)) {
                if (this.function.compute((FaunusEdge) edge)) {
                    newEdges.add(edge);
                    allowedCounter++;
                } else {
                    filterCounter++;
                }
            }
            value.setOutEdges(newEdges);
            context.write(NullWritable.get(), value);

            if (allowedCounter > 0)
                context.getCounter(Counters.EDGES_ALLOWED_BY_FUNCTION).increment(allowedCounter);
            if (filterCounter > 0)
                context.getCounter(Counters.EDGES_FILTERED_BY_FUNCTION).increment(filterCounter);
        }
    }
}
