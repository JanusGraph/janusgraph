package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.FaunusConfiguration;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexMap {

    public static final String IDS = Tokens.makeNamespace(VertexMap.class) + ".ids";

    public enum Counters {
        VERTICES_PROCESSED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private Collection<Long> ids;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.ids = FaunusConfiguration.getLongCollection(context.getConfiguration(), IDS, new HashSet<Long>());

        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            if (this.ids.contains(value.getIdAsLong())) {
                value.startPath();
            } else {
                value.clearPaths();
            }
            context.write(NullWritable.get(), value);
        }
    }
}
