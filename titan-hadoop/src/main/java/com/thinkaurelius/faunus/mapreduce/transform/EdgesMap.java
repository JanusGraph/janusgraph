package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgesMap {

    public static final String PROCESS_VERTICES = Tokens.makeNamespace(EdgesMap.class) + ".processVertices";

    public enum Counters {
        VERTICES_PROCESSED,
        OUT_EDGES_PROCESSED,
        IN_EDGES_PROCESSED
    }

    public static Configuration createConfiguration(final boolean processVertices) {
        final Configuration configuration = new EmptyConfiguration();
        configuration.setBoolean(PROCESS_VERTICES, processVertices);
        return configuration;
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private boolean processVertices;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.processVertices = context.getConfiguration().getBoolean(PROCESS_VERTICES, true);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {

            if (this.processVertices) {
                value.clearPaths();
                context.getCounter(Counters.VERTICES_PROCESSED).increment(1l);
            }

            long edgesProcessed = 0;
            for (final Edge edge : value.getEdges(Direction.IN)) {
                ((FaunusEdge) edge).startPath();
                edgesProcessed++;
            }
            context.getCounter(Counters.IN_EDGES_PROCESSED).increment(edgesProcessed);

            edgesProcessed = 0;
            for (final Edge edge : value.getEdges(Direction.OUT)) {
                ((FaunusEdge) edge).startPath();
                edgesProcessed++;
            }
            context.getCounter(Counters.OUT_EDGES_PROCESSED).increment(edgesProcessed);

            context.write(NullWritable.get(), value);
        }
    }
}
