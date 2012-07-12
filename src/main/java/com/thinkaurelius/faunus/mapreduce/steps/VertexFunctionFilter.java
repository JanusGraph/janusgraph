package com.thinkaurelius.faunus.mapreduce.steps;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexFunctionFilter {

    public static final String FUNCTION = Tokens.makeNamespace(VertexFunctionFilter.class) + ".function";

    public enum Counters {
        VERTICES_ALLOWED,
        VERTICES_FILTERED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private Function<FaunusVertex, Boolean> function;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            Class c = null;
            try {
                c = context.getConfiguration().getClass(FUNCTION, null);
                this.function = (Function<FaunusVertex, Boolean>) c.getConstructor().newInstance();
            } catch (Exception e) {
                throw new IOException("Unable to construct function: " + c);
            }
        }


        @Override
        public void map(final NullWritable key, final FaunusVertex value, final org.apache.hadoop.mapreduce.Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            if (this.function.compute(value)) {
                context.getCounter(Counters.VERTICES_ALLOWED).increment(1);
                context.write(NullWritable.get(), value);
            } else {
                context.getCounter(Counters.VERTICES_FILTERED).increment(1);
            }
        }
    }

}
