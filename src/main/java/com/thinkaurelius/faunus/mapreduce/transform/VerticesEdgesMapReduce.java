package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.FaunusConfiguration;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusElement;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.util.MicroEdge;
import com.thinkaurelius.faunus.util.MicroElement;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VerticesEdgesMapReduce {

    public static final String DIRECTION = Tokens.makeNamespace(VerticesEdgesMapReduce.class) + ".direction";
    public static final String LABELS = Tokens.makeNamespace(VerticesEdgesMapReduce.class) + ".labels";

    public enum Counters {
        EDGES_PROCESSED
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder> {

        private Direction direction;
        private String[] labels;

        private final Holder<FaunusElement> holder = new Holder<FaunusElement>();
        private final LongWritable longWritable = new LongWritable();
        private final FaunusEdge edge = new FaunusEdge();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.direction = FaunusConfiguration.getDirection(context.getConfiguration(), DIRECTION);
            this.labels = context.getConfiguration().getStrings(LABELS);

        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder>.Context context) throws IOException, InterruptedException {

            if (value.hasPaths()) {
                for (final Edge e : value.getEdges(this.direction, this.labels)) {
                    final FaunusEdge edge = (FaunusEdge) e;
                    final List<List<MicroElement>> paths = clonePaths(value, new MicroEdge(edge.getIdAsLong()));
                    edge.addPaths(paths, false);
                    this.edge.reuse(edge.getIdAsLong(), edge.getVertexId(Direction.OUT), edge.getVertexId(Direction.IN), edge.getLabel());
                    this.edge.addPaths(paths, false);
                    this.longWritable.set(edge.getVertexId(this.direction.opposite()));
                    context.write(this.longWritable, this.holder.set('p', this.edge));
                }
            }

            value.clearPaths();
            this.longWritable.set(value.getIdAsLong());
            context.write(this.longWritable, this.holder.set('v', value));
        }

        private List<List<MicroElement>> clonePaths(final FaunusVertex vertex, final MicroEdge edge) {
            final List<List<MicroElement>> paths = new ArrayList<List<MicroElement>>();
            for (List<MicroElement> path : vertex.getPaths()) {
                final List<MicroElement> p = new ArrayList<MicroElement>();
                p.addAll(path);
                p.add(edge);
                paths.add(p);
            }
            return paths;
        }

    }

    public static class Reduce extends Reducer<LongWritable, Holder, NullWritable, FaunusVertex> {

        private Direction direction;
        private String[] labels;

        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            this.direction = FaunusConfiguration.getDirection(context.getConfiguration(), DIRECTION);
            this.direction = this.direction.opposite();
            this.labels = context.getConfiguration().getStrings(LABELS);
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final FaunusVertex vertex = new FaunusVertex(key.get());  // TODO: make a FaunusVertex.clear() for object reuse
            final List<FaunusEdge> edges = new ArrayList<FaunusEdge>();
            for (final Holder holder : values) {
                final char tag = holder.getTag();
                if (tag == 'v') {
                    final FaunusVertex temp = (FaunusVertex) holder.get();
                    vertex.setProperties(temp.getProperties());
                    vertex.addEdges(Direction.BOTH, temp);
                } else {
                    edges.add((FaunusEdge) holder.get());
                }
            }
            for (final Edge e : vertex.getEdges(this.direction, this.labels)) {
                for (final FaunusEdge edge : edges) {
                    if (e.getId().equals(edge.getId())) {
                        ((FaunusEdge) e).addPaths(edge.getPaths(), false);
                        break;
                    }
                }
            }

            context.write(NullWritable.get(), vertex);
        }
    }
}
