package com.thinkaurelius.faunus.mapreduce.derivations;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CloseLine {

    public enum Counters {
        EDGES_TRANSPOSED
    }

    public static final String LABEL = Tokens.makeNamespace(CloseLine.class) + ".label";
    public static final String NEW_LABEL = Tokens.makeNamespace(CloseLine.class) + ".newLabel";
    public static final String ACTION = Tokens.makeNamespace(CloseLine.class) + ".action";
    public static final String OPPOSITE = Tokens.makeNamespace(CloseLine.class) + ".opposite";


    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private String label;
        private String newLabel;
        private Tokens.Action action;
        private boolean opposite;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.label = context.getConfiguration().get(LABEL);
            this.newLabel = context.getConfiguration().get(NEW_LABEL);
            this.action = Tokens.Action.valueOf(context.getConfiguration().get(ACTION));
            this.opposite = context.getConfiguration().getBoolean(OPPOSITE, true);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            long counter = 0;

            Iterator<FaunusEdge> itty = (Iterator) value.getEdges(OUT, this.label).iterator();
            while (itty.hasNext()) {
                final FaunusEdge edge = itty.next();
                if(this.opposite)
                    value.addEdge(IN, new FaunusEdge(edge.getVertexId(IN), edge.getVertexId(OUT), this.newLabel)).setProperties(edge.getProperties());
                else
                    value.addEdge(OUT, new FaunusEdge(edge.getVertexId(OUT), edge.getVertexId(IN), this.newLabel)).setProperties(edge.getProperties());
                counter++;

                if (action.equals(Tokens.Action.DROP))
                    itty.remove();
            }

            itty = (Iterator) value.getEdges(IN, this.label).iterator();
            while (itty.hasNext()) {
                final FaunusEdge edge = itty.next();

                if(this.opposite)
                    value.addEdge(OUT, new FaunusEdge(edge.getVertexId(IN), edge.getVertexId(OUT), this.newLabel)).setProperties(edge.getProperties());
                else
                    value.addEdge(IN, new FaunusEdge(edge.getVertexId(OUT), edge.getVertexId(IN), this.newLabel)).setProperties(edge.getProperties());

                counter++;

                if (action.equals(Tokens.Action.DROP))
                    itty.remove();
            }

            if (counter > 0)
                context.getCounter(Counters.EDGES_TRANSPOSED).increment(counter);

            context.write(NullWritable.get(), value);
        }
    }
}
