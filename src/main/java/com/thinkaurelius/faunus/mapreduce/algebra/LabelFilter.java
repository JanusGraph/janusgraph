package com.thinkaurelius.faunus.mapreduce.algebra;

import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LabelFilter {

    public static String[] edgeLabels;

    public static class Map extends Mapper<LongWritable, FaunusVertex, NullWritable, FaunusVertex> {
        @Override
        public void map(final LongWritable key, final FaunusVertex value, final org.apache.hadoop.mapreduce.Mapper<LongWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            value.setOutEdges((List) value.getOutEdges(edgeLabels));
            context.write(NullWritable.get(), value);
        }
    }

}
