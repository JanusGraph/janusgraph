package com.thinkaurelius.faunus.formats.titan;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.BlueprintsGraphOutputMapReduce;
import com.thinkaurelius.faunus.mapreduce.util.EmptyConfiguration;
import com.thinkaurelius.titan.core.DefaultTypeMaker;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.blueprints.BlueprintsDefaultTypeMaker;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SchemaInferencerMapReduce {

    public enum Counters {
        EDGE_LABELS_CREATED,
        PROPERTY_KEYS_CREATED
    }

    private static final long funnyLong = -123456789l;
    private static final LongWritable funnyKey = new LongWritable(funnyLong);

    public static Configuration createConfiguration() {
        return new EmptyConfiguration();
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, FaunusVertex> {

        private final FaunusVertex funnyVertex = new FaunusVertex(funnyKey.get());
        private final LongWritable longWritable = new LongWritable();

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            for (final String property : value.getPropertyKeys()) {
                this.funnyVertex.setProperty("t" + property, Object.class.getName());
                // TODO: Automated type inference
                /*final Object temp = this.funnyVertex.getProperty("t" + property);
                if (null == temp) {
                    this.funnyVertex.setProperty("t" + property, value.getProperty(property).getClass().getName());
                } else if (!value.getProperty(property).equals(temp)) {
                    this.funnyVertex.setProperty("t" + property, Object.class.getName());
                }*/
            }

            for (final Edge edge : value.getEdges(Direction.OUT)) {
                this.funnyVertex.setProperty("l" + edge.getLabel(), String.class.getName());
                for (final String property : edge.getPropertyKeys()) {
                    this.funnyVertex.setProperty("t" + property, Object.class.getName());
                }
            }

            this.longWritable.set(value.getIdAsLong());
            context.write(this.longWritable, value);
        }

        @Override
        public void cleanup(final Mapper<NullWritable, FaunusVertex, LongWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            context.write(funnyKey, this.funnyVertex);
        }
    }

    public static class Reduce extends org.apache.hadoop.mapreduce.Reducer<LongWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private TitanGraph graph;

        @Override
        public void setup(final Reduce.Context context) throws IOException, InterruptedException {
            this.graph = (TitanGraph) BlueprintsGraphOutputMapReduce.generateGraph(context.getConfiguration());
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<FaunusVertex> value, final Reducer<LongWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            if (key.get() == funnyLong) {
                final DefaultTypeMaker typeMaker = BlueprintsDefaultTypeMaker.INSTANCE;
                for (final FaunusVertex vertex : value) {
                    for (final String property : vertex.getPropertyKeys()) {
                        final String property2 = property.substring(1);
                        if (property.startsWith("t")) {
                            if (null == graph.getType(property2)) {
                                // TODO: Automated type inference
                                // typeMaker.makeKey(property2, graph.makeType().dataType(Class.forName(vertex.getProperty(property).toString())));
                                typeMaker.makeKey(property2, graph.makeType());
                                context.getCounter(Counters.PROPERTY_KEYS_CREATED).increment(1l);
                            }
                        } else {
                            if (null == graph.getType(property2)) {
                                typeMaker.makeLabel(property2, graph.makeType());
                                context.getCounter(Counters.EDGE_LABELS_CREATED).increment(1l);
                            }
                        }
                    }
                }
            } else {
                for (final FaunusVertex vertex : value) {
                    context.write(NullWritable.get(), vertex);
                }
            }
        }

        @Override
        public void cleanup(final Reducer<LongWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            this.graph.commit();
            this.graph.shutdown();
        }
    }
}
