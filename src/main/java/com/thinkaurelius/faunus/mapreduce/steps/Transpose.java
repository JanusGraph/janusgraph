package com.thinkaurelius.faunus.mapreduce.steps;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.util.Tokens;
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
public class Transpose {

    public enum Counters {
        EDGES_TRANSPOSED
    }

    public static final String LABEL = Tokens.makeNamespace(Traverse.class) + ".label";
    public static final String NEW_LABEL = Tokens.makeNamespace(Traverse.class) + ".newLabel";
    public static final String ACTION = Tokens.makeNamespace(Traverse.class) + ".action";


    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private String label;
        private String newLabel;
        private Tokens.Action action;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.label = context.getConfiguration().get(LABEL);
            this.newLabel = context.getConfiguration().get(NEW_LABEL);
            this.action = Tokens.Action.valueOf(context.getConfiguration().get(ACTION));
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            long counter = 0;

            final List<Edge> newInEdges = new ArrayList<Edge>();
            final List<Edge> outEdges = new ArrayList<Edge>();
            for (final Edge edge : value.getEdges(OUT)) {
                if (edge.getLabel().equals(this.label)) {
                    newInEdges.add(new FaunusEdge((FaunusVertex) edge.getVertex(IN), (FaunusVertex) edge.getVertex(OUT), this.newLabel));
                    counter++;
                    if (this.action.equals(Tokens.Action.KEEP))
                        outEdges.add(edge);
                } else {
                    outEdges.add(edge);
                }

            }

            final List<Edge> newOutEdges = new ArrayList<Edge>();
            final List<Edge> inEdges = new ArrayList<Edge>();
            for (final Edge edge : value.getEdges(IN)) {
                if (edge.getLabel().equals(this.label)) {
                    newOutEdges.add(new FaunusEdge((FaunusVertex) edge.getVertex(IN), (FaunusVertex) edge.getVertex(OUT), this.newLabel));
                    counter++;
                    if (this.action.equals(Tokens.Action.KEEP))
                        inEdges.add(edge);
                } else {
                    inEdges.add(edge);
                }
            }

            outEdges.addAll(newOutEdges);
            inEdges.addAll(newInEdges);
            value.setEdges(OUT, outEdges);
            value.setEdges(IN, inEdges);
            context.write(NullWritable.get(), value);

            if (counter > 0)
                context.getCounter(Counters.EDGES_TRANSPOSED).increment(counter);
        }
    }
}
