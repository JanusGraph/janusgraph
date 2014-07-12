package com.thinkaurelius.titan.hadoop.mapreduce;

import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge;
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
public class IdentityMap {

    public enum Counters {
        VERTEX_COUNT,
        OUT_EDGE_COUNT,
        IN_EDGE_COUNT,
        VERTEX_PROPERTY_COUNT,
        OUT_EDGE_PROPERTY_COUNT,
        IN_EDGE_PROPERTY_COUNT

    }

    public static Configuration createConfiguration() {
        return new EmptyConfiguration();
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {

            HadoopCompatLoader.getDefaultCompat().incrementContextCounter(context, Counters.VERTEX_COUNT, 1L);
            //context.getCounter(Counters.VERTEX_COUNT).increment(1l);
            HadoopCompatLoader.getDefaultCompat().incrementContextCounter(context, Counters.VERTEX_PROPERTY_COUNT, value.getPropertyCollection().size());
            //context.getCounter(Counters.VERTEX_PROPERTY_COUNT).increment(value.getProperties().size());

            long edgeCount = 0;
            long edgePropertyCount = 0;
            for (final Edge edge : value.getEdges(Direction.IN)) {
                edgeCount++;
                edgePropertyCount = edgePropertyCount + ((StandardFaunusEdge) edge).getPropertyCollection().size();
            }
            HadoopCompatLoader.getDefaultCompat().incrementContextCounter(context, Counters.IN_EDGE_COUNT, edgeCount);
            //context.getCounter(Counters.IN_EDGE_COUNT).increment(edgeCount);
            HadoopCompatLoader.getDefaultCompat().incrementContextCounter(context, Counters.IN_EDGE_PROPERTY_COUNT, edgePropertyCount);
            //context.getCounter(Counters.IN_EDGE_PROPERTY_COUNT).increment(edgePropertyCount);

            edgeCount = 0;
            edgePropertyCount = 0;
            for (final Edge edge : value.getEdges(Direction.OUT)) {
                edgeCount++;
                edgePropertyCount = edgePropertyCount + ((StandardFaunusEdge) edge).getPropertyCollection().size();
            }
            HadoopCompatLoader.getDefaultCompat().incrementContextCounter(context, Counters.OUT_EDGE_COUNT, edgeCount);
            //context.getCounter(Counters.OUT_EDGE_COUNT).increment(edgeCount);
            HadoopCompatLoader.getDefaultCompat().incrementContextCounter(context, Counters.OUT_EDGE_PROPERTY_COUNT, edgePropertyCount);
            //context.getCounter(Counters.OUT_EDGE_PROPERTY_COUNT).increment(edgePropertyCount);

            context.write(NullWritable.get(), value);

        }
    }
}
