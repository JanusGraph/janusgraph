package com.thinkaurelius.faunus.mapreduce.sideeffect;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AsMap {

    public static final String CLASS = Tokens.makeNamespace(AsMap.class) + ".class";
    public static final String TAG = Tokens.makeNamespace(AsMap.class) + ".tag";

    public enum Counters {
        EDGES_KEPT,
        EDGES_DROPPED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private boolean isVertex;
        private char tag;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.isVertex = context.getConfiguration().getClass(CLASS, Element.class, Element.class).equals(Vertex.class);
            this.tag = context.getConfiguration().get(TAG).charAt(0);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {

            /* if (this.isVertex) {
                if (value.hasPaths())
                    value.setTag(this.tag);
            } else {
                for (final Edge edge : value.getEdges(Direction.BOTH)) {
                    if (((FaunusEdge) edge).hasPaths())
                        ((FaunusEdge) edge).setTag(this.tag);
                }
            }*/

            context.write(NullWritable.get(), value);
        }
    }
}
