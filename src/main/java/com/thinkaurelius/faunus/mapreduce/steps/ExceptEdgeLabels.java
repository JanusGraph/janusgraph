package com.thinkaurelius.faunus.mapreduce.steps;

import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ExceptEdgeLabels {

    public static final String LABELS = Tokens.makeNamespace(ExceptEdgeLabels.class) + ".labels";

    public enum Counters {
        EDGES_ALLOWED,
        EDGES_FILTERED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        protected Set<String> illegalLabels;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            final String[] temp = context.getConfiguration().getStrings(LABELS);
            if (temp != null && temp.length > 0)
                this.illegalLabels = new HashSet<String>(Arrays.asList(temp));
            else
                this.illegalLabels = null;
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            if (null != this.illegalLabels) {
                long allowedCounter = 0;
                long filteredCounter = 0;

                List<Edge> newEdges = new ArrayList<Edge>();
                for (final Edge edge : value.getEdges(OUT)) {
                    if (!this.illegalLabels.contains(edge.getLabel())) {
                        newEdges.add(edge);
                        allowedCounter++;
                    } else {
                        filteredCounter++;
                    }
                }
                value.setEdges(OUT, newEdges);

                newEdges = new ArrayList<Edge>();
                for (final Edge edge : value.getEdges(IN)) {
                    if (!this.illegalLabels.contains(edge.getLabel())) {
                        newEdges.add(edge);
                        allowedCounter++;
                    } else {
                        filteredCounter++;
                    }
                }
                value.setEdges(IN, newEdges);

                context.getCounter(Counters.EDGES_ALLOWED).increment(allowedCounter);
                context.getCounter(Counters.EDGES_FILTERED).increment(filteredCounter);
            } else {
                context.getCounter(Counters.EDGES_ALLOWED).increment(((List) value.getEdges(IN)).size());
                context.getCounter(Counters.EDGES_ALLOWED).increment(((List) value.getEdges(OUT)).size());
            }
            context.write(NullWritable.get(), value);

        }
    }
}
