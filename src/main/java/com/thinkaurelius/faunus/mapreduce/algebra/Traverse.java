package com.thinkaurelius.faunus.mapreduce.algebra;

import com.thinkaurelius.faunus.io.graph.FaunusEdge;
import com.thinkaurelius.faunus.io.graph.FaunusElement;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.thinkaurelius.faunus.io.graph.util.Holder;
import com.thinkaurelius.faunus.io.graph.util.TaggedHolder;
import com.tinkerpop.blueprints.pgm.Edge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Traverse {

    public static final String LABELS = "faunus.algebra.traverse.labels";

    public static class Map1 extends Mapper<NullWritable, FaunusVertex, LongWritable, TaggedHolder> {

        private String[] labels;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.labels = context.getConfiguration().getStrings(LABELS);
            if (this.labels.length != 2) {
                throw new IOException("Two labels must be provided for traversing");
            }
        }


        @Override
        public void map(final NullWritable key, final FaunusVertex value, final org.apache.hadoop.mapreduce.Mapper<NullWritable, FaunusVertex, LongWritable, TaggedHolder>.Context context) throws IOException, InterruptedException {

            final FaunusVertex vertex = new FaunusVertex((Long) value.getId());
            vertex.setProperties(value.getProperties());
            context.write(new LongWritable((Long) vertex.getId()), new TaggedHolder<FaunusVertex>('v', vertex));

            for (final Edge edge : value.getOutEdges()) {
                if (edge.getLabel().equals(this.labels[0])) {
                    context.write(new LongWritable((Long) edge.getInVertex().getId()), new TaggedHolder<FaunusEdge>('a', (FaunusEdge) edge));
                } else if (edge.getLabel().equals(this.labels[1])) {
                    context.write(new LongWritable((Long) edge.getOutVertex().getId()), new TaggedHolder<FaunusEdge>('b', (FaunusEdge) edge));
                }
            }
        }
    }

    public static class Reduce1 extends Reducer<LongWritable, TaggedHolder, LongWritable, Holder> {

        private String[] labels;
        private String newLabel;

        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            this.labels = context.getConfiguration().getStrings(LABELS);
            if (this.labels.length != 2) {
                throw new IOException("Two labels must be provided for traversing");
            }
            this.newLabel = this.labels[0] + "-" + this.labels[1];
        }


        @Override
        public void reduce(final LongWritable key, final Iterable<TaggedHolder> values, final org.apache.hadoop.mapreduce.Reducer<LongWritable, TaggedHolder, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            final Configuration configuration = context.getConfiguration();
            final List<FaunusEdge> edgesA = new LinkedList<FaunusEdge>();
            final List<FaunusEdge> edgesB = new LinkedList<FaunusEdge>();

            for (final TaggedHolder taggedHolder : values) {
                final FaunusElement element = taggedHolder.get();
                final char tag = taggedHolder.getTag();
                if (tag == 'a')
                    edgesA.add(WritableUtils.clone((FaunusEdge) element, configuration));
                else if (tag == 'b')
                    edgesB.add(WritableUtils.clone((FaunusEdge) element, configuration));
                else if (tag == 'v')
                    context.write(key, new Holder<FaunusVertex>((FaunusVertex) element));
                else
                    throw new IOException("Tag value " + tag + " is an unknown tag");
            }


            for (final FaunusEdge edgeA : edgesA) {
                for (final FaunusEdge edgeB : edgesB) {
                    context.write(new LongWritable((Long) edgeA.getOutVertex().getId()), new Holder<FaunusEdge>(new FaunusEdge((FaunusVertex) edgeA.getOutVertex(), (FaunusVertex) edgeB.getInVertex(), this.newLabel)));
                }
            }
        }
    }

    public static class Map2 extends Mapper<LongWritable, Holder, LongWritable, Holder> {

        @Override
        public void map(final LongWritable key, final Holder value, final org.apache.hadoop.mapreduce.Mapper<LongWritable, Holder, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class Reduce2 extends Reducer<LongWritable, Holder, NullWritable, FaunusVertex> {

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final org.apache.hadoop.mapreduce.Reducer<LongWritable, Holder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final Configuration configuration = context.getConfiguration();
            final List<FaunusEdge> edges = new LinkedList<FaunusEdge>();
            FaunusVertex vertex = null;

            for (final Holder holder : values) {
                final FaunusElement element = holder.get();
                if (element instanceof FaunusEdge) {
                    edges.add(WritableUtils.clone((FaunusEdge) element, configuration));
                } else if (element instanceof FaunusVertex) {
                    vertex = WritableUtils.clone((FaunusVertex) element, configuration);
                }
            }

            if (null == vertex)
                throw new IOException("Vertex " + key + " not propagated in stream");
            else {
                for (final FaunusEdge edge : edges) {
                    vertex.addOutEdge(edge);
                }
                context.write(NullWritable.get(), vertex);
            }
        }
    }
}
