package com.thinkaurelius.faunus.mapreduce.steps;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.util.TaggedHolder;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

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


    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, TaggedHolder> {

        private String label;
        private String newLabel;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.label = context.getConfiguration().get(LABEL);
            this.newLabel = context.getConfiguration().get(NEW_LABEL);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, TaggedHolder>.Context context) throws IOException, InterruptedException {
            long counter = 0;
            final FaunusVertex vertex = value.cloneIdAndProperties();

            context.write(vertex.getIdAsLongWritable(), new TaggedHolder<FaunusVertex>('v', vertex));
            for (final Edge edge : value.getEdges(OUT)) {
                context.write(vertex.getIdAsLongWritable(), new TaggedHolder<FaunusEdge>('o', (FaunusEdge) edge));
                if (edge.getLabel().equals(this.label)) {
                    final FaunusEdge inverseEdge = new FaunusEdge((FaunusVertex) edge.getVertex(IN), (FaunusVertex) edge.getVertex(OUT), this.newLabel);
                    inverseEdge.setProperties(((FaunusEdge) edge).getProperties());
                    counter++;
                    context.write(new LongWritable((Long) inverseEdge.getVertex(OUT).getId()), new TaggedHolder<FaunusEdge>('i', inverseEdge));
                }
            }

            for (final Edge edge : value.getEdges(IN)) {
                context.write(vertex.getIdAsLongWritable(), new TaggedHolder<FaunusEdge>('i', (FaunusEdge) edge));
                if (edge.getLabel().equals(this.label)) {
                    final FaunusEdge inverseEdge = new FaunusEdge((FaunusVertex) edge.getVertex(IN), (FaunusVertex) edge.getVertex(OUT), this.newLabel);
                    inverseEdge.setProperties(((FaunusEdge) edge).getProperties());
                    counter++;
                    context.write(new LongWritable((Long) inverseEdge.getVertex(IN).getId()), new TaggedHolder<FaunusEdge>('o', inverseEdge));
                }
            }

            if (counter > 0)
                context.getCounter(Counters.EDGES_TRANSPOSED).increment(counter);
        }
    }

    public static class Reduce extends Reducer<LongWritable, TaggedHolder, NullWritable, FaunusVertex> {

        @Override
        public void reduce(final LongWritable key, final Iterable<TaggedHolder> values, final Reducer<LongWritable, TaggedHolder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final FaunusVertex vertex = new FaunusVertex(key.get());
            for (final TaggedHolder holder : values) {
                final char tag = holder.getTag();
                if (tag == 'v') {
                    vertex.setProperties(WritableUtils.clone(holder.get(), context.getConfiguration()).getProperties());
                } else if (tag == 'o') {
                    vertex.addEdge(OUT, WritableUtils.clone((FaunusEdge) holder.get(), context.getConfiguration()));
                } else if (tag == 'i') {
                    vertex.addEdge(IN, WritableUtils.clone((FaunusEdge) holder.get(), context.getConfiguration()));
                } else {
                    throw new IOException("A tag of " + tag + " is not a legal tag for this operation");
                }
            }
            context.write(NullWritable.get(), vertex);
        }
    }
}
