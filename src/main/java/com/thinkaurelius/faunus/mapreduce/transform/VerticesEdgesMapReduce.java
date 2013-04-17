package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusElement;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.faunus.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.tinkerpop.blueprints.Direction.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VerticesEdgesMapReduce {

    public static final String DIRECTION = Tokens.makeNamespace(VerticesEdgesMapReduce.class) + ".direction";
    public static final String LABELS = Tokens.makeNamespace(VerticesEdgesMapReduce.class) + ".labels";

    public enum Counters {
        EDGES_TRAVERSED
    }

    public static Configuration createConfiguration(final Direction direction, final String... labels) {
        final Configuration configuration = new EmptyConfiguration();
        configuration.set(DIRECTION, direction.name());
        configuration.setStrings(LABELS, labels);
        return configuration;
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder> {

        private Direction direction;
        private String[] labels;
        private boolean pathEnabled;

        private final Holder<FaunusElement> holder = new Holder<FaunusElement>();
        private final LongWritable longWritable = new LongWritable();
        private FaunusEdge edge;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.direction = Direction.valueOf(context.getConfiguration().get(DIRECTION));
            this.labels = context.getConfiguration().getStrings(LABELS, new String[0]);
            this.pathEnabled = context.getConfiguration().getBoolean(FaunusCompiler.PATH_ENABLED, false);
            this.edge = new FaunusEdge(this.pathEnabled);

        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder>.Context context) throws IOException, InterruptedException {

            if (value.hasPaths()) {
                long edgesTraversed = 0l;

                if (this.direction.equals(IN) || this.direction.equals(BOTH)) {
                    for (final Edge e : value.getEdges(IN, this.labels)) {
                        final FaunusEdge edge = (FaunusEdge) e;
                        this.edge.reuse(edge.getIdAsLong(), edge.getVertexId(OUT), edge.getVertexId(IN), edge.getLabel());

                        if (this.pathEnabled) {
                            final List<List<FaunusElement.MicroElement>> paths = clonePaths(value, new FaunusEdge.MicroEdge(edge.getIdAsLong()));
                            edge.addPaths(paths, false);
                            this.edge.addPaths(paths, false);
                        } else {
                            edge.getPaths(value, false);
                            this.edge.getPaths(value, false);
                        }
                        this.longWritable.set(edge.getVertexId(OUT));
                        context.write(this.longWritable, this.holder.set('p', this.edge));
                        edgesTraversed++;
                    }
                }

                if (this.direction.equals(OUT) || this.direction.equals(BOTH)) {
                    for (final Edge e : value.getEdges(OUT, this.labels)) {
                        final FaunusEdge edge = (FaunusEdge) e;
                        this.edge.reuse(edge.getIdAsLong(), edge.getVertexId(OUT), edge.getVertexId(IN), edge.getLabel());

                        if (this.pathEnabled) {
                            final List<List<FaunusElement.MicroElement>> paths = clonePaths(value, new FaunusEdge.MicroEdge(edge.getIdAsLong()));
                            edge.addPaths(paths, false);
                            this.edge.addPaths(paths, false);
                        } else {
                            edge.getPaths(value, false);
                            this.edge.getPaths(value, false);
                        }
                        this.longWritable.set(edge.getVertexId(IN));
                        context.write(this.longWritable, this.holder.set('p', this.edge));
                        edgesTraversed++;
                    }
                }

                value.clearPaths();
                context.getCounter(Counters.EDGES_TRAVERSED).increment(edgesTraversed);
            }


            this.longWritable.set(value.getIdAsLong());
            context.write(this.longWritable, this.holder.set('v', value));
        }

        // TODO: this is horribly inefficient due to an efficiency of object reuse in path calculations
        private List<List<FaunusElement.MicroElement>> clonePaths(final FaunusVertex vertex, final FaunusEdge.MicroEdge edge) {
            final List<List<FaunusElement.MicroElement>> paths = new ArrayList<List<FaunusElement.MicroElement>>();
            for (List<FaunusElement.MicroElement> path : vertex.getPaths()) {
                final List<FaunusElement.MicroElement> p = new ArrayList<FaunusElement.MicroElement>();
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
        private FaunusVertex vertex;

        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            this.direction = Direction.valueOf(context.getConfiguration().get(DIRECTION));
            if (!this.direction.equals(BOTH))
                this.direction = this.direction.opposite();

            this.labels = context.getConfiguration().getStrings(LABELS);
            this.vertex = new FaunusVertex(context.getConfiguration().getBoolean(FaunusCompiler.PATH_ENABLED, false));
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            this.vertex.reuse(key.get());
            final List<FaunusEdge> edges = new ArrayList<FaunusEdge>();
            for (final Holder holder : values) {
                final char tag = holder.getTag();
                if (tag == 'v') {
                    this.vertex.addAll((FaunusVertex) holder.get());
                } else {
                    edges.add((FaunusEdge) holder.get());
                }
            }

            for (final Edge e : this.vertex.getEdges(this.direction, this.labels)) {
                for (final FaunusEdge edge : edges) {
                    if (e.getId().equals(edge.getId())) {
                        ((FaunusEdge) e).getPaths(edge, false);
                        break;
                    }
                }
            }

            context.write(NullWritable.get(), this.vertex);
        }
    }
}