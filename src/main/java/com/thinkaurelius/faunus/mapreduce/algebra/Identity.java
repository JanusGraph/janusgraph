package com.thinkaurelius.faunus.mapreduce.algebra;

import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Identity {

    public enum Counters {
        VERTEX_COUNT
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final org.apache.hadoop.mapreduce.Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            context.getCounter(Counters.VERTEX_COUNT).increment(1);
            context.write(NullWritable.get(), value);
        }
    }
}
