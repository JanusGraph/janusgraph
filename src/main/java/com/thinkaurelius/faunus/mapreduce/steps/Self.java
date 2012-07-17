package com.thinkaurelius.faunus.mapreduce.steps;

import com.thinkaurelius.faunus.FaunusVertex;
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
public class Self {

    public static final String ACTION = Tokens.makeNamespace(Self.class) + ".action";

    public enum Counters {
        EDGES_ALLOWED,
        EDGES_FILTERED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {
        private Tokens.Action action;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.action = Tokens.Action.valueOf(context.getConfiguration().get(ACTION));
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            long allowedCounter = 0;
            long filteredCounter = 0;

            List<Edge> newEdges = new ArrayList<Edge>();
            for (final Edge edge : value.getEdges(OUT)) {
                if (this.action.equals(Tokens.Action.KEEP)) {
                    if (edge.getVertex(OUT).getId().equals(edge.getVertex(IN).getId())) {
                        newEdges.add(edge);
                        allowedCounter++;
                    } else {
                        filteredCounter++;
                    }
                } else {
                    if (!edge.getVertex(OUT).getId().equals(edge.getVertex(IN).getId())) {
                        newEdges.add(edge);
                        allowedCounter++;
                    } else {
                        filteredCounter++;
                    }
                }
            }
            value.setEdges(OUT, newEdges);

            newEdges = new ArrayList<Edge>();
            for (final Edge edge : value.getEdges(IN)) {
                if (this.action.equals(Tokens.Action.KEEP)) {
                    if (edge.getVertex(OUT).getId().equals(edge.getVertex(IN).getId())) {
                        newEdges.add(edge);
                        allowedCounter++;
                    } else {
                        filteredCounter++;
                    }
                } else {
                    if (!edge.getVertex(OUT).getId().equals(edge.getVertex(IN).getId())) {
                        newEdges.add(edge);
                        allowedCounter++;
                    } else {
                        filteredCounter++;
                    }
                }
            }
            value.setEdges(IN, newEdges);
            context.write(NullWritable.get(), value);

            context.getCounter(Counters.EDGES_ALLOWED).increment(allowedCounter);
            context.getCounter(Counters.EDGES_FILTERED).increment(filteredCounter);

        }
    }
}
