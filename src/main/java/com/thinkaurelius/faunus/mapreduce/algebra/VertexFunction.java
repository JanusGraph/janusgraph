package com.thinkaurelius.faunus.mapreduce.algebra;

import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexFunction {

    public static final String FUNCTION = Tokens.makeNamespace(VertexFunction.class) + ".function";

    public enum Counters {
        VERTICES_PROCESSED,
    }


    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private Function<FaunusVertex, FaunusVertex> function;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            Class c = null;
            try {
                c = context.getConfiguration().getClass(FUNCTION, null);
                this.function = (Function<FaunusVertex, FaunusVertex>) c.getConstructor().newInstance();
            } catch (Exception e) {
                throw new IOException("Unable to construct function: " + c);
            }
        }


        @Override
        public void map(final NullWritable key, final FaunusVertex value, final org.apache.hadoop.mapreduce.Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final FaunusVertex vertex = function.compute(value);
            context.getCounter(Counters.VERTICES_PROCESSED).increment(1);
            if (null != vertex)
                context.write(NullWritable.get(), vertex);
        }
    }


}
