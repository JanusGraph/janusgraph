package com.thinkaurelius.faunus.mapreduce.derivations;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;

import static com.tinkerpop.blueprints.Direction.BOTH;
import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Self {

    public static final String ACTION = Tokens.makeNamespace(Self.class) + ".action";
    public static final String LABELS = Tokens.makeNamespace(Self.class) + ".labels";

    public enum Counters {
        EDGES_DROPPED,
        EDGES_KEPT
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private String[] labels;
        private Tokens.Action action;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.labels = context.getConfiguration().getStrings(LABELS, new String[0]);
            this.action = Tokens.Action.valueOf(context.getConfiguration().get(ACTION));
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            long droppedCounter = 0l;
            long keptCounter = 0l;

            final Iterator<FaunusEdge> itty = (Iterator) value.getEdges(BOTH, this.labels).iterator();
            while (itty.hasNext()) {
                final FaunusEdge edge = itty.next();
                if (action.equals(Tokens.Action.KEEP)) {
                    if (edge.getVertexId(IN) != edge.getVertexId(OUT)) {
                        itty.remove();
                        droppedCounter++;
                    } else {
                        keptCounter++;
                    }
                } else {
                    if (edge.getVertexId(IN) == edge.getVertexId(OUT)) {
                        itty.remove();
                        droppedCounter++;
                    } else {
                        keptCounter++;
                    }
                }
            }

            context.getCounter(Counters.EDGES_DROPPED).increment(droppedCounter);
            context.getCounter(Counters.EDGES_KEPT).increment(keptCounter);
            context.write(NullWritable.get(), value);
        }

    }
}
