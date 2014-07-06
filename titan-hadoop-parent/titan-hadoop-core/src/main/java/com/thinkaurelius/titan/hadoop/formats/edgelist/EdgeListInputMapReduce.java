package com.thinkaurelius.titan.hadoop.formats.edgelist;

import com.thinkaurelius.titan.hadoop.HadoopEdge;
import com.thinkaurelius.titan.hadoop.HadoopElement;
import com.thinkaurelius.titan.hadoop.HadoopVertex;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompat;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import static com.tinkerpop.blueprints.Direction.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeListInputMapReduce {

    public enum Counters {
        EDGES_PROCESSED,
        VERTICES_EMITTED,
        IN_EDGES_CREATED,
        OUT_EDGES_CREATED,
        VERTICES_CREATED,
        VERTEX_PROPERTIES_CREATED
    }

    public static Configuration createConfiguration() {
        return new EmptyConfiguration();
    }

    public static class Map extends Mapper<NullWritable, HadoopElement, LongWritable, HadoopVertex> {

        private final HashMap<Long, HadoopVertex> map = new HashMap<Long, HadoopVertex>();
        private static final int MAX_MAP_SIZE = 5000;
        private final LongWritable longWritable = new LongWritable();
        private int counter = 0;

        @Override
        public void map(final NullWritable key, final HadoopElement value, final Mapper<NullWritable, HadoopElement, LongWritable, HadoopVertex>.Context context) throws IOException, InterruptedException {
            if (value instanceof HadoopEdge) {
                final long outId = ((HadoopEdge) value).getVertexId(OUT);
                final long inId = ((HadoopEdge) value).getVertexId(IN);
                HadoopVertex vertex = this.map.get(outId);
                if (null == vertex) {
                    vertex = new HadoopVertex(context.getConfiguration(), outId);
                    this.map.put(outId, vertex);
                }
                vertex.addEdge(OUT, WritableUtils.clone((HadoopEdge) value, context.getConfiguration()));
                this.counter++;

                vertex = this.map.get(inId);
                if (null == vertex) {
                    vertex = new HadoopVertex(context.getConfiguration(), inId);
                    this.map.put(inId, vertex);
                }
                vertex.addEdge(IN, WritableUtils.clone((HadoopEdge) value, context.getConfiguration()));
                HadoopCompatLoader.getDefaultCompat().incrementContextCounter(context, Counters.EDGES_PROCESSED, 1L);
                //context.getCounter(Counters.EDGES_PROCESSED).increment(1l);
                this.counter++;
            } else {
                final long id = value.getIdAsLong();
                HadoopVertex vertex = this.map.get(id);
                if (null == vertex) {
                    vertex = new HadoopVertex(context.getConfiguration(), id);
                    this.map.put(id, vertex);
                }
                vertex.addAllProperties(value.getProperties());
                vertex.addEdges(BOTH, WritableUtils.clone((HadoopVertex) value, context.getConfiguration()));
                this.counter++;
            }
            if (this.counter > MAX_MAP_SIZE)
                this.flush(context);
        }

        @Override
        public void cleanup(final Mapper<NullWritable, HadoopElement, LongWritable, HadoopVertex>.Context context) throws IOException, InterruptedException {
            this.flush(context);
        }

        private void flush(final Mapper<NullWritable, HadoopElement, LongWritable, HadoopVertex>.Context context) throws IOException, InterruptedException {
            for (final HadoopVertex vertex : this.map.values()) {
                this.longWritable.set(vertex.getIdAsLong());
                context.write(this.longWritable, vertex);
                HadoopCompatLoader.getDefaultCompat().incrementContextCounter(context, Counters.VERTICES_EMITTED, 1L);
                //context.getCounter(Counters.VERTICES_EMITTED).increment(1l);
            }
            this.map.clear();
            this.counter = 0;
        }
    }

    public static class Combiner extends Reducer<LongWritable, HadoopVertex, LongWritable, HadoopVertex> {

        @Override
        public void reduce(final LongWritable key, final Iterable<HadoopVertex> values, final Reducer<LongWritable, HadoopVertex, LongWritable, HadoopVertex>.Context context) throws IOException, InterruptedException {
            final HadoopVertex vertex = new HadoopVertex(context.getConfiguration(), key.get());
            for (final HadoopVertex value : values) {
                vertex.addEdges(BOTH, value);
                vertex.addAllProperties(value.getProperties());
            }
            context.write(key, vertex);
        }
    }

    public static class Reduce extends Reducer<LongWritable, HadoopVertex, NullWritable, HadoopVertex> {


        @Override
        public void reduce(final LongWritable key, final Iterable<HadoopVertex> values, final Reducer<LongWritable, HadoopVertex, NullWritable, HadoopVertex>.Context context) throws IOException, InterruptedException {
            final HadoopVertex vertex = new HadoopVertex(context.getConfiguration(), key.get());
            for (final HadoopVertex value : values) {
                vertex.addEdges(BOTH, value);
                vertex.addAllProperties(value.getProperties());
            }
            HadoopCompatLoader.getDefaultCompat().incrementContextCounter(context, Counters.VERTICES_CREATED, 1L);
            //context.getCounter(Counters.VERTICES_CREATED).increment(1l);
            HadoopCompatLoader.getDefaultCompat().incrementContextCounter(context, Counters.VERTEX_PROPERTIES_CREATED, vertex.getProperties().size());
            //context.getCounter(Counters.VERTEX_PROPERTIES_CREATED).increment(vertex.getProperties().size());
            HadoopCompatLoader.getDefaultCompat().incrementContextCounter(context, Counters.OUT_EDGES_CREATED, ((List)vertex.getEdges(OUT)).size());
            //context.getCounter(Counters.OUT_EDGES_CREATED).increment(((List) vertex.getEdges(OUT)).size());
            HadoopCompatLoader.getDefaultCompat().incrementContextCounter(context, Counters.IN_EDGES_CREATED, ((List) vertex.getEdges(IN)).size());
            //context.getCounter(Counters.IN_EDGES_CREATED).increment(((List) vertex.getEdges(IN)).size());
            context.write(NullWritable.get(), vertex);
        }
    }
}
