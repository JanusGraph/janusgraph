package com.thinkaurelius.titan.hadoop.mapreduce.transform;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.hadoop.*;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.tinkerpop.blueprints.Direction.*;

import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VerticesEdgesMapReduce {

    private static final Logger log =
            LoggerFactory.getLogger(VerticesEdgesMapReduce.class);

//    public static final String DIRECTION = Tokens.makeNamespace(VerticesEdgesMapReduce.class) + ".direction";
//    public static final String LABELS = Tokens.makeNamespace(VerticesEdgesMapReduce.class) + ".labels";

    public enum Counters {
        EDGES_TRAVERSED
    }

    public static org.apache.hadoop.conf.Configuration createConfiguration(final Direction direction, final String... labels) {
        ModifiableHadoopConfiguration c = ModifiableHadoopConfiguration.withoutResources();
        c.set(VERTICES_EDGES_DIRECTION, direction);
        c.set(VERTICES_EDGES_LABELS, labels);
        return c.getHadoopConfiguration();
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder> {

        private Direction direction;
        private String[] labels;
        private boolean trackPaths;
        private Configuration faunusConf;

        private final Holder<FaunusPathElement> holder = new Holder<FaunusPathElement>();
        private final LongWritable longWritable = new LongWritable();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            faunusConf = ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getJobContextConfiguration(context));
            direction = faunusConf.get(VERTICES_EDGES_DIRECTION);
            labels = faunusConf.get(VERTICES_EDGES_LABELS);
            trackPaths = faunusConf.get(PIPELINE_TRACK_PATHS);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder>.Context context) throws IOException, InterruptedException {

            if (log.isTraceEnabled())
                log.trace("{}.map: trackPaths={}", getClass().getSimpleName(), trackPaths);

            if (value.hasPaths()) {
                long edgesTraversed = 0l;

                if (direction.equals(IN) || direction.equals(BOTH)) {
                    for (final Edge e : value.getEdges(IN, labels)) {
                        final StandardFaunusEdge edge = (StandardFaunusEdge) e;
                        final StandardFaunusEdge shellEdge = new StandardFaunusEdge(faunusConf, edge.getLongId(), edge.getVertexId(OUT), edge.getVertexId(IN), edge.getLabel());


                        if (trackPaths) {
                            final List<List<FaunusPathElement.MicroElement>> paths = clonePaths(value, new StandardFaunusEdge.MicroEdge(edge.getLongId()));
                            edge.addPaths(paths, false);
                            shellEdge.addPaths(paths, false);
                        } else {
                            edge.getPaths(value, false);
                            shellEdge.getPaths(value, false);
                        }
                        longWritable.set(edge.getVertexId(OUT));
                        context.write(longWritable, holder.set('p', shellEdge));
                        edgesTraversed++;
                    }
                }

                if (direction.equals(OUT) || direction.equals(BOTH)) {
                    for (final Edge e : value.getEdges(OUT, labels)) {
                        final StandardFaunusEdge edge = (StandardFaunusEdge) e;
                        final StandardFaunusEdge shellEdge = new StandardFaunusEdge(faunusConf, edge.getLongId(), edge.getVertexId(OUT), edge.getVertexId(IN), edge.getLabel());

                        if (trackPaths) {
                            final List<List<FaunusPathElement.MicroElement>> paths = clonePaths(value, new StandardFaunusEdge.MicroEdge(edge.getLongId()));
                            edge.addPaths(paths, false);
                            shellEdge.addPaths(paths, false);
                            log.trace("shellEdge pathCount={} for edgelabel={}", shellEdge.pathCount(), e.getLabel());
                        } else {
                            edge.getPaths(value, false);
                            shellEdge.getPaths(value, false);
                        }
                        longWritable.set(edge.getVertexId(IN));
                        context.write(longWritable, holder.set('p', shellEdge));
                        edgesTraversed++;
                    }
                }

                value.clearPaths();
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.EDGES_TRAVERSED, edgesTraversed);
            }


            longWritable.set(value.getLongId());
            context.write(longWritable, holder.set('v', value));
        }

        // TODO: this is horribly inefficient due to an efficiency of object reuse in path calculations
        private List<List<FaunusPathElement.MicroElement>> clonePaths(final FaunusVertex vertex, final StandardFaunusEdge.MicroEdge edge) {
            final List<List<FaunusPathElement.MicroElement>> paths = new ArrayList<List<FaunusPathElement.MicroElement>>();
            for (List<FaunusPathElement.MicroElement> path : vertex.getPaths()) {
                final List<FaunusPathElement.MicroElement> p = new ArrayList<FaunusPathElement.MicroElement>();
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
        private Configuration faunusConf;

        @Override
        public void setup(final Reducer.Context context) throws IOException, InterruptedException {
            faunusConf = ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getContextConfiguration(context));
            direction = faunusConf.get(VERTICES_EDGES_DIRECTION);

            if (!direction.equals(BOTH))
                direction = direction.opposite();

            labels = faunusConf.get(VERTICES_EDGES_LABELS);
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            final FaunusVertex vertex = new FaunusVertex(faunusConf, key.get());
            final List<StandardFaunusEdge> edges = new ArrayList<StandardFaunusEdge>();
            for (final Holder holder : values) {
                final char tag = holder.getTag();
                if (tag == 'v') {
                    vertex.addAll((FaunusVertex) holder.get());
                } else {
                    edges.add((StandardFaunusEdge) holder.get());
                }
            }

            for (final Edge e : vertex.getEdges(direction, labels)) {
                StandardFaunusEdge fe = (StandardFaunusEdge)e;
                for (final StandardFaunusEdge edge : edges) {
                    if (fe.getLongId()==edge.getLongId()) {
                        fe.getPaths(edge, false);
                        break;
                    }
                }


                if (log.isTraceEnabled())
                    log.trace("{}.reduce: edge={} pathCount={}", getClass().getSimpleName(), fe, fe.pathCount());
            }

            context.write(NullWritable.get(), vertex);
        }
    }
}