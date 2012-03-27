package com.thinkaurelius.faunus.mapreduce.algebra;

import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LabelFilter {

    public static final String LABELS_PROPERTY = "faunus.algebra.labelfilter.labels";


    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private String[] labels;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.labels = context.getConfiguration().getStrings(LABELS_PROPERTY);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final org.apache.hadoop.mapreduce.Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            value.setOutEdges((List) value.getOutEdges(this.labels));
            context.write(NullWritable.get(), value);
        }
    }

}
