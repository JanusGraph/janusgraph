package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.util.MicroElement;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathMap {

    public static final String CLASS = Tokens.makeNamespace(PathMap.class) + ".class";

    public enum Counters {
        VERTICES_PROCESSED,
        EDGES_PROCESSED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, Text> {

        private boolean isVertex;
        private final Text textWritable = new Text();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.isVertex = context.getConfiguration().getClass(CLASS, Element.class, Element.class).equals(Vertex.class);
        }


        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            if (this.isVertex && value.hasPaths()) {
                for (final List<MicroElement> path : value.getPaths()) {
                    this.textWritable.set(path.toString());
                    context.write(NullWritable.get(), this.textWritable);
                }
            } else {
                for (final Edge e : value.getEdges(Direction.OUT)) {
                    final FaunusEdge edge = (FaunusEdge) e;
                    if (edge.hasPaths()) {
                        for (final List<MicroElement> path : edge.getPaths()) {
                            this.textWritable.set(path.toString());
                            context.write(NullWritable.get(), this.textWritable);
                        }
                    }
                }
            }
        }
    }
}
