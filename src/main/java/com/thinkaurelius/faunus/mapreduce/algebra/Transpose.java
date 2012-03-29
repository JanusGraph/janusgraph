package com.thinkaurelius.faunus.mapreduce.algebra;

import com.thinkaurelius.faunus.io.graph.FaunusEdge;
import com.thinkaurelius.faunus.io.graph.FaunusElement;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.thinkaurelius.faunus.io.graph.util.Holder;
import com.thinkaurelius.faunus.mapreduce.algebra.util.Counters;
import com.tinkerpop.blueprints.pgm.Edge;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Transpose {

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder> {

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final org.apache.hadoop.mapreduce.Mapper<NullWritable, FaunusVertex, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            long counter = 0;
            final Long vertexId = (Long) value.getId();
            final FaunusVertex vertex = new FaunusVertex(vertexId);
            vertex.setProperties(value.getProperties());
            context.write(new LongWritable(vertexId), new Holder<FaunusVertex>(vertex));
            for (final Edge edge : value.getOutEdges()) {
                final FaunusEdge inverseEdge = new FaunusEdge((FaunusVertex) edge.getInVertex(), (FaunusVertex) edge.getOutVertex(), edge.getLabel() + "_inv");
                inverseEdge.setProperties(((FaunusEdge) edge).getProperties());
                counter++;
                context.write(new LongWritable((Long) inverseEdge.getOutVertex().getId()), new Holder<FaunusEdge>(inverseEdge));
            }
            if (counter > 0)
                context.getCounter(Counters.EDGES_TRANSPOSED).increment(counter);
        }
    }

    public static class Reduce extends Reducer<LongWritable, Holder, NullWritable, FaunusVertex> {

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final org.apache.hadoop.mapreduce.Reducer<LongWritable, Holder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final FaunusVertex vertex = new FaunusVertex(key.get());
            for (final Holder holder : values) {
                final FaunusElement element = holder.get();
                if (element instanceof FaunusEdge) {
                    vertex.addOutEdge(WritableUtils.clone((FaunusEdge) element, context.getConfiguration()));
                } else if (element instanceof FaunusVertex) {
                    vertex.setProperties(element.getProperties());
                } else {
                    throw new IOException("Illegal element type propagated through the stream: " + element.getClass());
                }
            }
            context.write(NullWritable.get(), vertex);
        }
    }
}
