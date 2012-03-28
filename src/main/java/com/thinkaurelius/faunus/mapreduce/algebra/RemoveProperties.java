package com.thinkaurelius.faunus.mapreduce.algebra;

import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RemoveProperties {

    public static final String KEYS_PROPERTY = "faunus.algebra.removeproperties.keys";


    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private String[] keys;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.keys = context.getConfiguration().getStrings(KEYS_PROPERTY);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final org.apache.hadoop.mapreduce.Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            if (keys.length == 0) {
                for (final String k : value.getPropertyKeys()) {
                    value.removeProperty(k);
                }
            } else {
                for (final String k : this.keys) {
                    value.removeProperty(k);
                }
            }
            context.write(NullWritable.get(), value);
        }
    }
}
