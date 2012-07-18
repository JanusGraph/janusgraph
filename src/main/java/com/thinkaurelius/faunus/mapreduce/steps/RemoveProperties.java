package com.thinkaurelius.faunus.mapreduce.steps;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.util.Tokens;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RemoveProperties {

    public static final String KEYS = Tokens.makeNamespace(RemoveProperties.class) + ".properties";

    public enum Counters {
        PROPERTIES_REMOVED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        protected String[] keys;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.keys = context.getConfiguration().getStrings(KEYS);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            long counter = 0;
            if (null == this.keys || this.keys.length == 0) {
                for (final String k : value.getPropertyKeys()) {
                    counter++;
                    value.removeProperty(k);
                }
            } else {
                for (final String k : this.keys) {
                    if (null != value.removeProperty(k))
                        counter++;
                }
            }

            if (counter > 0)
                context.getCounter(Counters.PROPERTIES_REMOVED).increment(counter);
            context.write(NullWritable.get(), value);
        }
    }
}
