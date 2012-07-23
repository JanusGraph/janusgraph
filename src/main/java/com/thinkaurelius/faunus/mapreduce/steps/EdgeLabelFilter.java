package com.thinkaurelius.faunus.mapreduce.steps;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.util.Tokens;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

import static com.tinkerpop.blueprints.Direction.BOTH;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeLabelFilter {

    public static final String LABELS = Tokens.makeNamespace(EdgeLabelFilter.class) + ".labels";
    public static final String ACTION = Tokens.makeNamespace(EdgeLabelFilter.class) + ".action";

    public enum Counters {
        EDGES_KEPT,
        EDGES_DROPPED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private String[] labels;
        private Tokens.Action action;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.labels = context.getConfiguration().getStrings(LABELS);
            this.action = Tokens.Action.valueOf(context.getConfiguration().get(ACTION));
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            long originalSize = ((List) value.getEdges(BOTH)).size();
            value.removeEdges(this.action, BOTH, this.labels);
            long newSize = ((List) value.getEdges(BOTH)).size();

            context.getCounter(Counters.EDGES_KEPT).increment(originalSize - (originalSize - newSize));
            context.getCounter(Counters.EDGES_DROPPED).increment(originalSize - newSize);

            context.write(NullWritable.get(), value);

        }
    }
}
