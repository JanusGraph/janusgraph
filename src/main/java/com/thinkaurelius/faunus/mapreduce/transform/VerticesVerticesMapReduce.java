package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.FaunusConfiguration;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VerticesVerticesMapReduce {

    public static final String DIRECTION = Tokens.makeNamespace(VerticesVerticesMapReduce.class) + ".direction";
    public static final String LABELS = Tokens.makeNamespace(VerticesVerticesMapReduce.class) + ".labels";

    public enum Counters {
        VERTICES_PROCESSED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder> {

        private Direction direction;
        private String[] labels;

        private final Holder<FaunusVertex> holder = new Holder<FaunusVertex>();
        private final FaunusVertex vertex = new FaunusVertex(); // TODO: make a FaunusVertex.getVerticesIds()
        private final LongWritable longWritable = new LongWritable();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.direction = FaunusConfiguration.getDirection(context.getConfiguration(), DIRECTION);
            this.labels = context.getConfiguration().getStrings(LABELS);

        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            if (value.hasPaths()) {
                for (final Edge edge : value.getEdges(this.direction, this.labels)) {
                    FaunusVertex vertex = new FaunusVertex((Long) edge.getVertex(this.direction.opposite()).getId());
                    for(List<Long> path : value.getPaths()) {
                        vertex.addPath(path);
                    }
                    this.longWritable.set(vertex.getIdAsLong());
                    context.write(this.longWritable, this.holder.set('e', vertex));
                }

                /*for (final Vertex v : value.getVertices(this.direction, this.labels)) {
                    FaunusVertex vertex = (FaunusVertex) v;
                    vertex.setEnergy(energy);
                    this.longWritable.set(vertex.getIdAsLong());
                    context.write(this.longWritable, this.holder.set('e', vertex));
                }*/
            }
            value.clearPaths();
            this.longWritable.set(value.getIdAsLong());
            context.write(this.longWritable, this.holder.set('o', value));
        }
    }

    public static class Reduce extends Reducer<LongWritable, Holder, NullWritable, FaunusVertex> {

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final FaunusVertex vertex = new FaunusVertex(key.get());  // TODO: make a FaunusVertex.clear() for object reuse
            for (final Holder holder : values) {
                final char tag = holder.getTag();
                final FaunusVertex vertex2 = (FaunusVertex) holder.get();
                if (tag == 'o') {
                    vertex.setProperties(vertex2.getProperties());
                    for (final Edge edge : vertex2.getEdges(OUT)) {
                        vertex.addEdge(OUT, (FaunusEdge) edge);
                    }
                    for (final Edge edge : vertex2.getEdges(IN)) {
                        vertex.addEdge(IN, (FaunusEdge) edge);
                    }
                } else if (tag == 'e') {
                    for(List<Long> path : vertex2.getPaths()) {
                        path.add(vertex.getIdAsLong());
                        vertex.addPath(path);
                    }
                } else {
                    throw new IOException("A tag of " + tag + " is not a legal tag for this operation");
                }
            }
            context.write(NullWritable.get(), vertex);
        }
    }
}
