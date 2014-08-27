package com.thinkaurelius.titan.hadoop.formats;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;
import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.INPUT_EDGE_COPY_DIRECTION;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge;
import com.thinkaurelius.titan.hadoop.Holder;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeCopyMapReduce {

    public enum Counters {
        EDGES_COPIED,
        EDGES_ADDED
    }

    public static org.apache.hadoop.conf.Configuration createConfiguration(final Direction direction) {
        org.apache.hadoop.conf.Configuration c = new org.apache.hadoop.conf.Configuration(false);
        c.set(ConfigElement.getPath(INPUT_EDGE_COPY_DIRECTION), direction.name());
        return c;
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>> {

        private final Holder<FaunusVertex> vertexHolder = new Holder<FaunusVertex>();
        private final LongWritable longWritable = new LongWritable();
        private Direction direction = Direction.OUT;
        private ModifiableHadoopConfiguration faunusConf;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            faunusConf = ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getContextConfiguration(context));
            direction = faunusConf.getEdgeCopyDirection();
            if (direction.equals(Direction.BOTH))
                throw new InterruptedException(ExceptionFactory.bothIsNotSupported().getMessage());
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>>.Context context) throws IOException, InterruptedException {
            long edgesInverted = 0;

            for (final Edge edge : value.getEdges(this.direction)) {
                final long id = (Long) edge.getVertex(this.direction.opposite()).getId();
                final FaunusVertex shellVertex = new FaunusVertex(faunusConf, id);
                this.longWritable.set(id);
                shellVertex.addEdge(this.direction.opposite(), (StandardFaunusEdge) edge);
                context.write(this.longWritable, this.vertexHolder.set('s', shellVertex));
                edgesInverted++;
            }
            this.longWritable.set(value.getLongId());
            context.write(this.longWritable, this.vertexHolder.set('r', value));
            DEFAULT_COMPAT.incrementContextCounter(context, Counters.EDGES_COPIED, edgesInverted);
        }

    }

    public static class Reduce extends Reducer<LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex> {

        private Direction direction = Direction.OUT;
        private ModifiableHadoopConfiguration faunusConf;

        private static final Logger log =
                LoggerFactory.getLogger(Reduce.class);

        @Override
        public void setup(final Reduce.Context context) throws IOException, InterruptedException {
            faunusConf = ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getContextConfiguration(context));
            direction = faunusConf.getEdgeCopyDirection();
            if (direction.equals(Direction.BOTH))
                throw new InterruptedException(ExceptionFactory.bothIsNotSupported().getMessage());
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder<FaunusVertex>> values, final Reducer<LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            long edgesAggregated = 0;
            final FaunusVertex vertex = new FaunusVertex(faunusConf, key.get());
            for (final Holder<FaunusVertex> holder : values) {
                if (holder.getTag() == 's') {
                    edgesAggregated = edgesAggregated + Iterables.size(holder.get().getEdges(direction.opposite()));
                    //log.debug("Copying edges with direction {} from {} to {}", direction.opposite(), holder.get(), vertex);
                    vertex.addEdges(direction.opposite(), holder.get());
                    //log.trace("In-edge count after copy on {}: {}", vertex, Iterators.size(vertex.getEdges(Direction.IN).iterator()));
                } else {
                    //log.debug("Calling addAll on vertex {} with parameter vertex {}", vertex, holder.get());
                    vertex.addAll(holder.get());
                    //log.trace("In-edge count after addAll on {}: {}", vertex, Iterators.size(vertex.getEdges(Direction.IN).iterator()));
                }
            }
            context.write(NullWritable.get(), vertex);
            DEFAULT_COMPAT.incrementContextCounter(context, Counters.EDGES_ADDED, edgesAggregated);
        }
    }
}
