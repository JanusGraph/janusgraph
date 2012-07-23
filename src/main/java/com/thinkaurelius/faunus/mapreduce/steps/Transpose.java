package com.thinkaurelius.faunus.mapreduce.steps;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.util.Tokens;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

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

            for (final Edge edge : value.getEdges(OUT, this.label)) {
                value.addEdge(IN, new FaunusEdge((FaunusVertex) edge.getVertex(IN), (FaunusVertex) edge.getVertex(OUT), this.newLabel));
                counter++;
            }
            if (action.equals(Tokens.Action.DROP))
                value.removeEdges(Tokens.Action.DROP, OUT, this.label);


            for (final Edge edge : value.getEdges(IN, this.label)) {
                value.addEdge(OUT, new FaunusEdge((FaunusVertex) edge.getVertex(IN), (FaunusVertex) edge.getVertex(OUT), this.newLabel));
                counter++;
            }
            if (action.equals(Tokens.Action.DROP))
                value.removeEdges(Tokens.Action.DROP, IN, this.label);


            if (counter > 0)
                context.getCounter(Counters.EDGES_TRANSPOSED).increment(counter);
            
            context.write(NullWritable.get(), value);
        }
    }
}
