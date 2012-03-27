package com.thinkaurelius.faunus.mapreduce.algebra;

import com.thinkaurelius.faunus.io.graph.FaunusEdge;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.tinkerpop.blueprints.pgm.Edge;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Traverse {

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, FaunusEdge> {

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final org.apache.hadoop.mapreduce.Mapper<NullWritable, FaunusVertex, LongWritable, FaunusEdge>.Context context) throws IOException, InterruptedException {
            for (final Edge edge : value.getOutEdges()) {
                if (edge.getLabel().equals("knows")) {
                    context.write(new LongWritable((Long) edge.getInVertex().getId()), (FaunusEdge) edge);
                } else if (edge.getLabel().equals("created")) {
                    context.write(new LongWritable((Long) edge.getOutVertex().getId()), (FaunusEdge) edge);
                }
            }
        }
    }

    public static class Reduce extends Reducer<LongWritable, FaunusEdge, NullWritable, FaunusVertex> {

        @Override
        public void reduce(final LongWritable key, final Iterable<FaunusEdge> values, final org.apache.hadoop.mapreduce.Reducer<LongWritable, FaunusEdge, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final List<FaunusEdge> edges = new LinkedList<FaunusEdge>();
            for (final FaunusEdge edge : values) {
                edges.add(new FaunusEdge(edge));
            }

            for (int i = 0; i < edges.size(); i++) {
                final FaunusEdge edge1 = edges.get(i);
                for (int j = i + 1; j < edges.size(); j++) {
                    final FaunusEdge edge2 = edges.get(j);
                    if (!edge1.getLabel().equals(edge2.getLabel())) {
                        if (edge1.getLabel().equals("knows")) {
                            FaunusEdge temp = new FaunusEdge((FaunusVertex) edge1.getOutVertex(), (FaunusVertex) edge2.getInVertex(), "knows-created");
                            FaunusVertex v = new FaunusVertex(1l);
                            v.addOutEdge(temp);
                            context.write(NullWritable.get(), v);
                        } else {
                            FaunusEdge temp = new FaunusEdge((FaunusVertex) edge2.getOutVertex(), (FaunusVertex) edge1.getInVertex(), "knows-created");
                            FaunusVertex v = new FaunusVertex(1l);
                            v.addOutEdge(temp);
                            context.write(NullWritable.get(), v);
                        }
                    }
                }
            }
        }
    }
}
