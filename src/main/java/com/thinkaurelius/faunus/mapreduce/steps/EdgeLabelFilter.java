package com.thinkaurelius.faunus.mapreduce.steps;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.util.Tokens;
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
public class EdgeLabelFilter {

    public static final String LABELS = Tokens.makeNamespace(EdgeLabelFilter.class) + ".labels";
    public static final String ACTION = Tokens.makeNamespace(EdgeLabelFilter.class) + ".action";

    public enum Counters {
        EDGES_ALLOWED,
        EDGES_FILTERED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private Set<String> labels;
        private Tokens.Action action;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            final String[] strings = context.getConfiguration().getStrings(LABELS);
            if (null == strings || strings.length == 0)
                this.labels = new HashSet<String>();
            else
                this.labels = new HashSet<String>(Arrays.asList(strings));
            this.action = Tokens.Action.valueOf(context.getConfiguration().get(ACTION));
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {

            long allowedCounter = 0;
            long filteredCounter = 0;

            List<Edge> newEdges = new ArrayList<Edge>();
            for (final Edge edge : value.getEdges(OUT)) {
                if (this.action.equals(Tokens.Action.KEEP)) {
                    if (this.labels.contains(edge.getLabel())) {
                        newEdges.add(edge);
                        allowedCounter++;
                    } else {
                        filteredCounter++;
                    }
                } else {
                    if (this.labels.contains(edge.getLabel())) {
                        filteredCounter++;

                    } else {
                        newEdges.add(edge);
                        allowedCounter++;
                    }
                }
            }
            value.setEdges(OUT, newEdges);

            newEdges = new ArrayList<Edge>();
            for (final Edge edge : value.getEdges(IN)) {
                if (this.action.equals(Tokens.Action.KEEP)) {
                    if (this.labels.contains(edge.getLabel())) {
                        newEdges.add(edge);
                        allowedCounter++;
                    } else {
                        filteredCounter++;
                    }
                } else {
                    if (this.labels.contains(edge.getLabel())) {
                        filteredCounter++;
                    } else {
                        newEdges.add(edge);
                        allowedCounter++;
                    }
                }
            }
            value.setEdges(IN, newEdges);

            context.getCounter(Counters.EDGES_ALLOWED).increment(allowedCounter);
            context.getCounter(Counters.EDGES_FILTERED).increment(filteredCounter);

            context.write(NullWritable.get(), value);

        }
    }
}
