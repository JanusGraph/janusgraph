package com.thinkaurelius.titan.hadoop.mapreduce.transform;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VerticesMap {

    public static final String PROCESS_EDGES = Tokens.makeNamespace(VerticesMap.class) + ".processEdges";

    public enum Counters {
        VERTICES_PROCESSED,
        EDGES_PROCESSED
    }

    public static Configuration createConfiguration(final boolean processEdges) {
        final Configuration configuration = new EmptyConfiguration();
        configuration.setBoolean(PROCESS_EDGES, processEdges);
        return configuration;
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private boolean processEdges;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.processEdges = context.getConfiguration().getBoolean(PROCESS_EDGES, true);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            value.startPath();
            long edgesProcessed = 0;
            if (this.processEdges) {
                for (final Edge edge : value.getEdges(Direction.BOTH)) {
                    ((StandardFaunusEdge) edge).clearPaths();
                    edgesProcessed++;
                }

            }

            DEFAULT_COMPAT.incrementContextCounter(context, Counters.EDGES_PROCESSED, edgesProcessed);
            DEFAULT_COMPAT.incrementContextCounter(context, Counters.VERTICES_PROCESSED, 1L);
            context.write(NullWritable.get(), value);

        }
    }
}
