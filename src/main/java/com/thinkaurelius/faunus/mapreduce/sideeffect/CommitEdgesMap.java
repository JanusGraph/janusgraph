package com.thinkaurelius.faunus.mapreduce.sideeffect;

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
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CommitEdgesMap {

    public static final String ACTION = Tokens.makeNamespace(CommitEdgesMap.class) + ".action";

    public enum Counters {
        OUT_EDGES_DROPPED,
        OUT_EDGES_KEPT,
        IN_EDGES_DROPPED,
        IN_EDGES_KEPT
    }

    public static Configuration createConfiguration(final Tokens.Action action) {
        final Configuration configuration = new EmptyConfiguration();
        configuration.set(ACTION, action.name());
        return configuration;
    }


    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private boolean drop;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.drop = Tokens.Action.valueOf(context.getConfiguration().get(ACTION)).equals(Tokens.Action.DROP);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            Iterator<Edge> itty = value.getEdges(Direction.IN).iterator();
            long edgesKept = 0;
            long edgesDropped = 0;
            while (itty.hasNext()) {
                if (this.drop) {
                    if ((((FaunusEdge) itty.next()).hasPaths())) {
                        itty.remove();
                        edgesDropped++;
                    } else
                        edgesKept++;
                } else {
                    if (!(((FaunusEdge) itty.next()).hasPaths())) {
                        itty.remove();
                        edgesDropped++;
                    } else
                        edgesKept++;
                }
            }
            context.getCounter(Counters.IN_EDGES_DROPPED).increment(edgesDropped);
            context.getCounter(Counters.IN_EDGES_KEPT).increment(edgesKept);

            ///////////////////

            itty = value.getEdges(Direction.OUT).iterator();
            edgesKept = 0;
            edgesDropped = 0;
            while (itty.hasNext()) {
                if (this.drop) {
                    if ((((FaunusEdge) itty.next()).hasPaths())) {
                        itty.remove();
                        edgesDropped++;
                    } else
                        edgesKept++;
                } else {
                    if (!(((FaunusEdge) itty.next()).hasPaths())) {
                        itty.remove();
                        edgesDropped++;
                    } else
                        edgesKept++;
                }
            }
            context.getCounter(Counters.OUT_EDGES_DROPPED).increment(edgesDropped);
            context.getCounter(Counters.OUT_EDGES_KEPT).increment(edgesKept);

            context.write(NullWritable.get(), value);
        }
    }
}
