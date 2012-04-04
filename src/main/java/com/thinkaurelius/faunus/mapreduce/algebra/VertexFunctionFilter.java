package com.thinkaurelius.faunus.mapreduce.algebra;

import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.algebra.util.Counters;
import com.thinkaurelius.faunus.mapreduce.algebra.util.Function;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexFunctionFilter {

    public static final String FUNCTION = "faunus.algebra.vertexfunctionfilter.function";

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
                context.write(NullWritable.get(), value);
                context.getCounter(Counters.VERTICES_ALLOWED_BY_FUNCTION).increment(1);
            } else {
                context.getCounter(Counters.VERTICES_FILTERED_BY_FUNCTION).increment(1);
            }
        }
    }

}
