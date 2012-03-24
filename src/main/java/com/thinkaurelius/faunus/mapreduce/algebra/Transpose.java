package com.thinkaurelius.faunus.mapreduce.algebra;

import com.thinkaurelius.faunus.io.graph.FaunusEdge;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.tinkerpop.blueprints.pgm.Edge;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Transpose {

    private static String newLabel;

    public static class Map extends Mapper<LongWritable, FaunusVertex, LongWritable, FaunusEdge> {

        @Override
        public void map(final LongWritable key, final FaunusVertex value, final org.apache.hadoop.mapreduce.Mapper<LongWritable, FaunusVertex, LongWritable, FaunusEdge>.Context context) throws IOException, InterruptedException {
            for (final Edge edge : value.getOutEdges()) {
                context.write(new LongWritable((Long) edge.getInVertex().getId()), new FaunusEdge(-1l, (FaunusVertex) edge.getInVertex(), (FaunusVertex) edge.getOutVertex(), edge.getLabel() + "_inv"));
            }
        }
    }

    public static class Reduce extends Reducer<LongWritable, FaunusEdge, NullWritable, FaunusVertex> {

        @Override
        public void reduce(final LongWritable key, final Iterable<FaunusEdge> values, final org.apache.hadoop.mapreduce.Reducer<LongWritable, FaunusEdge, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final FaunusVertex vertex = new FaunusVertex(key.get());
            for (final FaunusEdge edge : values) {
                vertex.addOutEdge(new FaunusEdge(edge));
            }
            context.write(NullWritable.get(), vertex);
        }
    }
}
