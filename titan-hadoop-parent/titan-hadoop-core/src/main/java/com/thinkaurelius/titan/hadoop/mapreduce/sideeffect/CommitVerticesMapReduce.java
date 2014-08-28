package com.thinkaurelius.titan.hadoop.mapreduce.sideeffect;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.Holder;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CommitVerticesMapReduce {

//    public static final String ACTION = Tokens.makeNamespace(CommitVerticesMapReduce.class) + ".action";

    public enum Counters {
        VERTICES_KEPT,
        VERTICES_DROPPED,
        OUT_EDGES_KEPT,
        IN_EDGES_KEPT
    }

    public static org.apache.hadoop.conf.Configuration createConfiguration(final Tokens.Action action) {
        ModifiableHadoopConfiguration c = ModifiableHadoopConfiguration.withoutResources();
        c.set(COMMIT_VERTICES_ACTION, action);
        return c.getHadoopConfiguration();
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder> {

        private boolean drop;
        private final Holder<FaunusVertex> holder = new Holder<FaunusVertex>();
        private final LongWritable longWritable = new LongWritable();
        private Configuration faunusConf;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            faunusConf = ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getContextConfiguration(context));
            Tokens.Action configuredAction = faunusConf.get(COMMIT_VERTICES_ACTION);
            drop = Tokens.Action.DROP.equals(configuredAction);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            final boolean keep;
            final boolean hasPaths = value.hasPaths();

            long verticesKept = 0;
            long verticesDropped = 0;

            if (this.drop && hasPaths)
                keep = false;
            else if (!this.drop && hasPaths)
                keep = true;
            else
                keep = this.drop && !hasPaths;

            if (keep) {
                this.longWritable.set(value.getLongId());
                context.write(this.longWritable, this.holder.set('v', value));
                verticesKept++;
            } else {
                final long vertexId = value.getLongId();
                this.holder.set('k', new FaunusVertex(faunusConf, vertexId));

                Iterator<Edge> itty = value.getEdges(OUT).iterator();
                while (itty.hasNext()) {
                    Edge edge = itty.next();
                    final Long id = (Long) edge.getVertex(IN).getId();
                    if (!id.equals(vertexId)) {
                        this.longWritable.set(id);
                        context.write(this.longWritable, this.holder);
                    }
                }

                itty = value.getEdges(IN).iterator();
                while (itty.hasNext()) {
                    Edge edge = itty.next();
                    final Long id = (Long) edge.getVertex(OUT).getId();
                    if (!id.equals(vertexId)) {
                        this.longWritable.set(id);
                        context.write(this.longWritable, this.holder);
                    }
                }
                this.longWritable.set(value.getLongId());
                context.write(this.longWritable, this.holder.set('d', value));
                verticesDropped++;
            }

            DEFAULT_COMPAT.incrementContextCounter(context, Counters.VERTICES_DROPPED, verticesDropped);
            DEFAULT_COMPAT.incrementContextCounter(context, Counters.VERTICES_KEPT, verticesKept);
        }
    }

    public static class Combiner extends Reducer<LongWritable, Holder, LongWritable, Holder> {

        private final Holder<FaunusVertex> holder = new Holder<FaunusVertex>();
        private Configuration faunusConf;

        @Override
        public void setup(final Combiner.Context context) {
            faunusConf = ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getContextConfiguration(context));
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, LongWritable, Holder>.Context context) throws IOException, InterruptedException {
            FaunusVertex vertex = null;
            final Set<Long> ids = new HashSet<Long>();

            boolean isDeleted = false;
            for (final Holder holder : values) {
                char tag = holder.getTag();
                if (tag == 'k') {
                    ids.add(holder.get().getLongId());
                    // todo: once vertex is found, do individual removes to save memory
                } else {
                    vertex = (FaunusVertex) holder.get();
                    isDeleted = tag == 'd';
                }
            }
            if (null != vertex) {
                if (ids.size() > 0)
                    vertex.removeEdgesToFrom(ids);
                context.write(key, this.holder.set(isDeleted ? 'd' : 'v', vertex));
            } else {
                // vertex not on the same machine as the vertices being deleted
                for (final Long id : ids) {
                    context.write(key, this.holder.set('k', new FaunusVertex(faunusConf, id)));
                }
            }

        }
    }

    public static class Reduce extends Reducer<LongWritable, Holder, NullWritable, FaunusVertex> {

        private boolean trackState;

        @Override
        public void setup(final Reducer.Context context) {
            this.trackState = context.getConfiguration().getBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_STATE, false);
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder> values, final Reducer<LongWritable, Holder, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            FaunusVertex vertex = null;
            final Set<Long> ids = new HashSet<Long>();
            for (final Holder holder : values) {
                final char tag = holder.getTag();
                if (tag == 'k') {
                    ids.add(holder.get().getLongId());
                    // todo: once vertex is found, do individual removes to save memory
                } else if (tag == 'v') {
                    vertex = (FaunusVertex) holder.get();
                } else {
                    vertex = (FaunusVertex) holder.get();
                    Iterator<Edge> itty = vertex.getEdges(Direction.BOTH).iterator();
                    while (itty.hasNext()) {
                        itty.next();
                        itty.remove();
                    }
                    vertex.updateLifeCycle(ElementLifeCycle.Event.REMOVED);
                }
            }
            if (null != vertex) {
                if (ids.size() > 0)
                    vertex.removeEdgesToFrom(ids);

                if (this.trackState)
                    context.write(NullWritable.get(), vertex);
                else if (!vertex.isRemoved())
                    context.write(NullWritable.get(), vertex);

                DEFAULT_COMPAT.incrementContextCounter(context, Counters.OUT_EDGES_KEPT, Iterables.size(vertex.getEdges(OUT)));
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.IN_EDGES_KEPT, Iterables.size(vertex.getEdges(IN)));
            }
        }
    }

}
