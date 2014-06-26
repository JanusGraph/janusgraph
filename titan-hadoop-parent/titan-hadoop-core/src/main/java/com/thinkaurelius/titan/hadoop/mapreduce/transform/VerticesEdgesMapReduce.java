package com.thinkaurelius.titan.hadoop.mapreduce.transform;

import com.thinkaurelius.titan.hadoop.HadoopEdge;
import com.thinkaurelius.titan.hadoop.HadoopPathElement;
import com.thinkaurelius.titan.hadoop.HadoopVertex;
import com.thinkaurelius.titan.hadoop.Holder;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;
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

    public static class Map extends Mapper<NullWritable, HadoopVertex, LongWritable, Holder> {

        private Direction direction;
        private String[] labels;
        private boolean trackPaths;

        private final Holder<HadoopPathElement> holder = new Holder<HadoopPathElement>();
        private final LongWritable longWritable = new LongWritable();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.direction = Direction.valueOf(context.getConfiguration().get(DIRECTION));
            this.labels = context.getConfiguration().getStrings(LABELS, new String[0]);
            this.trackPaths = context.getConfiguration().getBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_PATHS, false);
        }

        @Override
        public void map(final NullWritable key, final HadoopVertex value, final Mapper<NullWritable, HadoopVertex, LongWritable, Holder>.Context context) throws IOException, InterruptedException {

            if (value.hasPaths()) {
                long edgesTraversed = 0l;

                if (this.direction.equals(IN) || this.direction.equals(BOTH)) {
                    for (final Edge e : value.getEdges(IN, this.labels)) {
                        final HadoopEdge edge = (HadoopEdge) e;
                        final HadoopEdge shellEdge = new HadoopEdge(context.getConfiguration(), edge.getIdAsLong(), edge.getVertexId(OUT), edge.getVertexId(IN), edge.getLabel());


                        if (this.trackPaths) {
                            final List<List<HadoopPathElement.MicroElement>> paths = clonePaths(value, new HadoopEdge.MicroEdge(edge.getIdAsLong()));
                            edge.addPaths(paths, false);
                            shellEdge.addPaths(paths, false);
                        } else {
                            edge.getPaths(value, false);
                            shellEdge.getPaths(value, false);
                        }
                        this.longWritable.set(edge.getVertexId(OUT));
                        context.write(this.longWritable, this.holder.set('p', shellEdge));
                        edgesTraversed++;
                    }
                }

                if (this.direction.equals(OUT) || this.direction.equals(BOTH)) {
                    for (final Edge e : value.getEdges(OUT, this.labels)) {
                        final HadoopEdge edge = (HadoopEdge) e;
                        final HadoopEdge shellEdge = new HadoopEdge(context.getConfiguration(), edge.getIdAsLong(), edge.getVertexId(OUT), edge.getVertexId(IN), edge.getLabel());

                        if (this.trackPaths) {
                            final List<List<HadoopPathElement.MicroElement>> paths = clonePaths(value, new HadoopEdge.MicroEdge(edge.getIdAsLong()));
                            edge.addPaths(paths, false);
                            shellEdge.addPaths(paths, false);
                        } else {
                            edge.getPaths(value, false);
                            shellEdge.getPaths(value, false);
                        }
                        this.longWritable.set(edge.getVertexId(IN));
                        context.write(this.longWritable, this.holder.set('p', shellEdge));
                        edgesTraversed++;
                    }
                }

                value.clearPaths();
                HadoopCompatLoader.getDefaultCompat().incrementContextCounter(context, Counters.EDGES_TRAVERSED, edgesTraversed);
//                context.getCounter(Counters.EDGES_TRAVERSED).increment(edgesTraversed);
            }


            this.longWritable.set(value.getIdAsLong());
            context.write(this.longWritable, this.holder.set('v', value));
        }

        // TODO: this is horribly inefficient due to an efficiency of object reuse in path calculations
        private List<List<HadoopPathElement.MicroElement>> clonePaths(final HadoopVertex vertex, final HadoopEdge.MicroEdge edge) {
            final List<List<HadoopPathElement.MicroElement>> paths = new ArrayList<List<HadoopPathElement.MicroElement>>();
            for (List<HadoopPathElement.MicroElement> path : vertex.getPaths()) {
                final List<HadoopPathElement.MicroElement> p = new ArrayList<HadoopPathElement.MicroElement>();
                p.addAll(path);
                p.add(edge);
                paths.add(p);
            }
            return paths;
        }

    }

    public static class Reduce extends Reducer<LongWritable, Holder, NullWritable, HadoopVertex> {

        private Direction direction;
        private String[] labels;

        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            this.direction = Direction.valueOf(context.getConfiguration().get(DIRECTION));
            if (!this.direction.equals(BOTH))
                this.direction = this.direction.opposite();

            this.labels = context.getConfiguration().getStrings(LABELS);
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, NullWritable, HadoopVertex>.Context context) throws IOException, InterruptedException {
            final HadoopVertex vertex = new HadoopVertex(context.getConfiguration(), key.get());
            final List<HadoopEdge> edges = new ArrayList<HadoopEdge>();
            for (final Holder holder : values) {
                final char tag = holder.getTag();
                if (tag == 'v') {
                    vertex.addAll((HadoopVertex) holder.get());
                } else {
                    edges.add((HadoopEdge) holder.get());
                }
            }

            for (final Edge e : vertex.getEdges(this.direction, this.labels)) {
                for (final HadoopEdge edge : edges) {
                    if (e.getId().equals(edge.getId())) {
                        ((HadoopEdge) e).getPaths(edge, false);
                        break;
                    }
                }
            }

            context.write(NullWritable.get(), vertex);
        }
    }
}