package com.thinkaurelius.faunus.mapreduce.sideeffect;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CommitEdgesMap {

    public static final String ACTION = Tokens.makeNamespace(CommitEdgesMap.class) + ".action";

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private boolean drop;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.drop = Tokens.Action.valueOf(context.getConfiguration().get(ACTION)).equals(Tokens.Action.DROP);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final Iterator<Edge> itty = value.getEdges(Direction.BOTH).iterator();
            while (itty.hasNext()) {
                if (this.drop) {
                    if ((((FaunusEdge) itty.next()).hasEnergy()))
                        itty.remove();
                } else {
                    if (!(((FaunusEdge) itty.next()).hasEnergy()))
                        itty.remove();
                }
            }
            context.write(NullWritable.get(), value);
        }
    }
}
