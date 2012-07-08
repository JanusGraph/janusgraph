package com.thinkaurelius.faunus.mapreduce.algebra;

import com.thinkaurelius.faunus.io.graph.FaunusEdge;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.thinkaurelius.faunus.io.graph.util.TaggedHolder;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Traverse {

    public static final String LABELS = Tokens.makeNamespace(Traverse.class) + ".labels";


    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, TaggedHolder> {

        private String[] labels;
        private String newLabel;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.labels = context.getConfiguration().getStrings(LABELS);
            if (this.labels.length != 2) {
                throw new IOException("Two labels must be provided for traversing");
            }
            this.newLabel = this.labels[0] + "-" + this.labels[1];
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final org.apache.hadoop.mapreduce.Mapper<NullWritable, FaunusVertex, LongWritable, TaggedHolder>.Context context) throws IOException, InterruptedException {
            Set<Long> outVertexIds = new HashSet<Long>();
            Set<Long> inVertexIds = new HashSet<Long>();

            for (final Edge edge : value.getEdges(IN)) {
                context.write(value.getIdAsLongWritable(), new TaggedHolder<FaunusEdge>('i', (FaunusEdge) edge));
                if (edge.getLabel().equals(this.labels[0])) {
                    outVertexIds.add((Long) edge.getVertex(OUT).getId());
                }
            }

            for (final Edge edge : value.getEdges(OUT)) {
                context.write(value.getIdAsLongWritable(), new TaggedHolder<FaunusEdge>('o', (FaunusEdge) edge));
                if (edge.getLabel().equals(this.labels[1])) {
                    inVertexIds.add((Long) edge.getVertex(IN).getId());
                }
            }

            for (final Long outId : outVertexIds) {
                for (Long inId : inVertexIds) {
                    final FaunusEdge edge = new FaunusEdge(new FaunusVertex(outId), new FaunusVertex(inId), newLabel);
                    context.write(new LongWritable(outId), new TaggedHolder<FaunusEdge>('o', edge));
                    context.write(new LongWritable(inId), new TaggedHolder<FaunusEdge>('i', edge));
                }
            }

            context.write(value.getIdAsLongWritable(), new TaggedHolder<FaunusVertex>('v', value.cloneIdAndProperties()));
        }
    }

    public static class Reduce extends Reducer<LongWritable, TaggedHolder, NullWritable, FaunusVertex> {
        @Override
        public void reduce(final LongWritable key, final Iterable<TaggedHolder> values, final org.apache.hadoop.mapreduce.Reducer<LongWritable, TaggedHolder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final FaunusVertex vertex = new FaunusVertex(key.get());
            for (final TaggedHolder holder : values) {
                final char tag = holder.getTag();
                if (tag == 'v') {
                    vertex.setProperties(holder.get().getProperties());
                } else if (tag == 'i') {
                    vertex.addEdge(IN, (FaunusEdge) holder.get());
                } else if (tag == 'o') {
                    vertex.addEdge(OUT, (FaunusEdge) holder.get());
                } else {
                    throw new IOException("A tag of " + tag + " is not a legal tag for this operation");
                }
            }
            context.write(NullWritable.get(), vertex);
        }
    }
}
