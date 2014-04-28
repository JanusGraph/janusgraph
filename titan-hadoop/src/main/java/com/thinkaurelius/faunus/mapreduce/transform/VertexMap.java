package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.util.EmptyConfiguration;
import org.apache.hadoop.conf.Configuration;
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

    public static Configuration createConfiguration(final long... ids) {
        final String[] idStrings = new String[ids.length];
        for (int i = 0; i < ids.length; i++) {
            idStrings[i] = String.valueOf(ids[i]);
        }
        final Configuration configuration = new EmptyConfiguration();
        configuration.setStrings(IDS, idStrings);
        return configuration;
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private Collection<Long> ids;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            //todo: make as list and double up repeats
            this.ids = VertexMap.Map.getLongCollection(context.getConfiguration(), IDS, new HashSet<Long>());
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            if (this.ids.contains(value.getIdAsLong())) {
                value.startPath();
                context.getCounter(Counters.VERTICES_PROCESSED).increment(1l);
            } else {
                value.clearPaths();
            }
            context.write(NullWritable.get(), value);
        }

        private static Collection<Long> getLongCollection(final Configuration conf, final String key, final Collection<Long> collection) {
            for (final String value : conf.getStrings(key)) {
                collection.add(Long.valueOf(value));
            }
            return collection;
        }
    }
}
