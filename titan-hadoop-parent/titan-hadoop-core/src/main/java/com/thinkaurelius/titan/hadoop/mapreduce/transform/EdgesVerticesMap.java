package com.thinkaurelius.titan.hadoop.mapreduce.transform;

import com.thinkaurelius.titan.hadoop.StandardFaunusEdge;
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

import static com.tinkerpop.blueprints.Direction.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgesVerticesMap {

    public static final String DIRECTION = Tokens.makeNamespace(EdgesVerticesMap.class) + ".direction";

    public enum Counters {
        IN_EDGES_PROCESSED,
        OUT_EDGES_PROCESSED
    }

    public static Configuration createConfiguration(final Direction direction) {
        final Configuration configuration = new EmptyConfiguration();
        configuration.set(DIRECTION, direction.name());
        return configuration;
    }

    public static class Map extends Mapper<NullWritable, HadoopVertex, NullWritable, HadoopVertex> {

        private Direction direction;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.direction = Direction.valueOf(context.getConfiguration().get(DIRECTION));
        }

        @Override
        public void map(final NullWritable key, final HadoopVertex value, final Mapper<NullWritable, HadoopVertex, NullWritable, HadoopVertex>.Context context) throws IOException, InterruptedException {

            if (this.direction.equals(IN) || this.direction.equals(BOTH)) {
                long edgesProcessed = 0;
                for (final Edge e : value.getEdges(IN)) {
                    final StandardFaunusEdge edge = (StandardFaunusEdge) e;
                    if (edge.hasPaths()) {
                        value.getPaths(edge, true);
                        edgesProcessed++;
                        edge.clearPaths();
                    }
                }
                HadoopCompatLoader.getDefaultCompat().incrementContextCounter(context, Counters.IN_EDGES_PROCESSED, edgesProcessed);
//                context.getCounter(Counters.IN_EDGES_PROCESSED).increment(edgesProcessed);
            } else {
                for (final Edge e : value.getEdges(IN)) {
                    final StandardFaunusEdge edge = (StandardFaunusEdge) e;
                    if (edge.hasPaths()) {
                        edge.clearPaths();
                    }
                }
            }

            if (this.direction.equals(OUT) || this.direction.equals(BOTH)) {
                long edgesProcessed = 0;
                for (final Edge e : value.getEdges(OUT)) {
                    final StandardFaunusEdge edge = (StandardFaunusEdge) e;
                    if (edge.hasPaths()) {
                        value.getPaths(edge, true);
                        edgesProcessed++;
                        edge.clearPaths();
                    }
                }
                HadoopCompatLoader.getDefaultCompat().incrementContextCounter(context, Counters.OUT_EDGES_PROCESSED, edgesProcessed);
//                context.getCounter(Counters.OUT_EDGES_PROCESSED).increment(edgesProcessed);
            } else {
                for (final Edge e : value.getEdges(OUT)) {
                    final StandardFaunusEdge edge = (StandardFaunusEdge) e;
                    if (edge.hasPaths()) {
                        edge.clearPaths();
                    }
                }
            }

            context.write(NullWritable.get(), value);

        }
    }

}
