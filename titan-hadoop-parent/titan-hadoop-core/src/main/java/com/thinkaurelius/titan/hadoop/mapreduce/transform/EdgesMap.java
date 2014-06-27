package com.thinkaurelius.titan.hadoop.mapreduce.transform;

import com.thinkaurelius.titan.hadoop.HadoopEdge;
import com.thinkaurelius.titan.hadoop.HadoopVertex;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader;
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

    public static class Map extends Mapper<NullWritable, HadoopVertex, NullWritable, HadoopVertex> {

        private boolean processVertices;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.processVertices = context.getConfiguration().getBoolean(PROCESS_VERTICES, true);
        }

        @Override
        public void map(final NullWritable key, final HadoopVertex value, final Mapper<NullWritable, HadoopVertex, NullWritable, HadoopVertex>.Context context) throws IOException, InterruptedException {

            if (this.processVertices) {
                value.clearPaths();
                HadoopCompatLoader.getDefaultCompat().incrementContextCounter(context, Counters.VERTICES_PROCESSED, 1L);
//                context.getCounter(Counters.VERTICES_PROCESSED).increment(1l);
            }

            long edgesProcessed = 0;
            for (final Edge edge : value.getEdges(Direction.IN)) {
                ((HadoopEdge) edge).startPath();
                edgesProcessed++;
            }
            HadoopCompatLoader.getDefaultCompat().incrementContextCounter(context, Counters.IN_EDGES_PROCESSED, edgesProcessed);
//            context.getCounter(Counters.IN_EDGES_PROCESSED).increment(edgesProcessed);

            edgesProcessed = 0;
            for (final Edge edge : value.getEdges(Direction.OUT)) {
                ((HadoopEdge) edge).startPath();
                edgesProcessed++;
            }
            HadoopCompatLoader.getDefaultCompat().incrementContextCounter(context, Counters.OUT_EDGES_PROCESSED, edgesProcessed);
//            context.getCounter(Counters.OUT_EDGES_PROCESSED).increment(edgesProcessed);

            context.write(NullWritable.get(), value);
        }
    }
}
