package com.thinkaurelius.faunus.mapreduce.algebra;

import com.thinkaurelius.faunus.io.graph.FaunusEdge;
import com.thinkaurelius.faunus.io.graph.FaunusElement;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.thinkaurelius.faunus.io.graph.util.ElementHolder;
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

    public static final String LABELS_PROPERTY = "faunus.algebra.traverse.labels";

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, ElementHolder> {

        private String[] labels;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.labels = context.getConfiguration().getStrings(LABELS_PROPERTY);
            if (this.labels.length != 2) {
                throw new IOException("Two labels must be provided for traversing");
            }
        }


        @Override
        public void map(final NullWritable key, final FaunusVertex value, final org.apache.hadoop.mapreduce.Mapper<NullWritable, FaunusVertex, LongWritable, ElementHolder>.Context context) throws IOException, InterruptedException {

            final FaunusVertex vertex = new FaunusVertex((Long) value.getId());
            vertex.setProperties(value.getProperties());
            context.write(new LongWritable((Long) vertex.getId()), new ElementHolder<FaunusVertex>(vertex));

            for (final Edge edge : value.getOutEdges()) {
                if (edge.getLabel().equals(this.labels[0])) {
                    context.write(new LongWritable((Long) edge.getInVertex().getId()), new ElementHolder<FaunusEdge>((FaunusEdge) edge));
                } else if (edge.getLabel().equals(this.labels[1])) {
                    context.write(new LongWritable((Long) edge.getOutVertex().getId()), new ElementHolder<FaunusEdge>((FaunusEdge) edge));
                }
            }
        }
    }

    public static class Reduce extends Reducer<LongWritable, ElementHolder, NullWritable, FaunusVertex> {

        private String[] labels;
        private String newLabel;

        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            this.labels = context.getConfiguration().getStrings(LABELS_PROPERTY);
            if (this.labels.length != 2) {
                throw new IOException("Two labels must be provided for traversing");
            }
            this.newLabel = this.labels[0] + "-" + this.labels[1];
        }


        @Override
        public void reduce(final LongWritable key, final Iterable<ElementHolder> values, final org.apache.hadoop.mapreduce.Reducer<LongWritable, ElementHolder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final List<FaunusEdge> edgesA = new LinkedList<FaunusEdge>();
            final List<FaunusEdge> edgesB = new LinkedList<FaunusEdge>();
            final Configuration configuration = context.getConfiguration();
            FaunusVertex vertex = null;


            for (final ElementHolder holder : values) {
                final FaunusElement element = holder.get();
                if (element instanceof FaunusEdge) {
                    final FaunusEdge edge = (FaunusEdge) element;
                    if (edge.getLabel().equals(this.labels[0]))
                        edgesA.add(WritableUtils.clone(edge, configuration));
                    else
                        edgesB.add(WritableUtils.clone(edge, configuration));
                } else if (element instanceof FaunusVertex) {
                    vertex = WritableUtils.clone((FaunusVertex) element, configuration);
                }
            }

            if (null == vertex) {
                throw new IOException("Vertex " + key + " not propagated in stream");
            }

            for (final FaunusEdge edgeA : edgesA) {
                for (final FaunusEdge edgeB : edgesB) {
                    final FaunusEdge temp = new FaunusEdge((FaunusVertex) edgeA.getOutVertex(), (FaunusVertex) edgeB.getInVertex(), this.newLabel);
                    vertex.addOutEdge(temp);
                }
            }
            context.write(NullWritable.get(), vertex);
        }
    }
}
