package com.thinkaurelius.titan.hadoop.formats.titan;

import com.thinkaurelius.titan.core.schema.DefaultSchemaMaker;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.blueprints.BlueprintsDefaultSchemaMaker;
import com.thinkaurelius.titan.hadoop.HadoopVertex;
import com.thinkaurelius.titan.hadoop.formats.BlueprintsGraphOutputMapReduce;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;
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

    public static class Map extends Mapper<NullWritable, HadoopVertex, LongWritable, HadoopVertex> {

        private HadoopVertex funnyVertex;
        private final LongWritable longWritable = new LongWritable();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.funnyVertex = new HadoopVertex(context.getConfiguration(), funnyLong);
        }

        @Override
        public void map(final NullWritable key, final HadoopVertex value, final Mapper<NullWritable, HadoopVertex, LongWritable, HadoopVertex>.Context context) throws IOException, InterruptedException {
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
        public void cleanup(final Mapper<NullWritable, HadoopVertex, LongWritable, HadoopVertex>.Context context) throws IOException, InterruptedException {
            context.write(funnyKey, this.funnyVertex);
        }
    }

    public static class Reduce extends org.apache.hadoop.mapreduce.Reducer<LongWritable, HadoopVertex, NullWritable, HadoopVertex> {

        private TitanGraph graph;

        @Override
        public void setup(final Reduce.Context context) throws IOException, InterruptedException {
            this.graph = (TitanGraph) BlueprintsGraphOutputMapReduce.generateGraph(context.getConfiguration());
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<HadoopVertex> value, final Reducer<LongWritable, HadoopVertex, NullWritable, HadoopVertex>.Context context) throws IOException, InterruptedException {
            if (key.get() == funnyLong) {
                final DefaultSchemaMaker typeMaker = BlueprintsDefaultSchemaMaker.INSTANCE;
                for (final HadoopVertex vertex : value) {
                    for (final String property : vertex.getPropertyKeys()) {
                        final String property2 = property.substring(1);
                        if (property.startsWith("t")) {
                            if (null == graph.getRelationType(property2)) {
                                // TODO: Automated type inference
                                // typeMaker.makeKey(property2, graph.makeType().dataType(Class.forName(vertex.getProperty(property).toString())));
                                typeMaker.makePropertyKey(graph.makePropertyKey(property2));
                                context.getCounter(Counters.PROPERTY_KEYS_CREATED).increment(1l);
                            }
                        } else {
                            if (null == graph.getRelationType(property2)) {
                                //typeMaker.makeLabel(property2, graph.makeType());
                                typeMaker.makeEdgeLabel(graph.makeEdgeLabel(property2));
                                context.getCounter(Counters.EDGE_LABELS_CREATED).increment(1l);
                            }
                        }
                    }
                }
            } else {
                for (final HadoopVertex vertex : value) {
                    context.write(NullWritable.get(), vertex);
                }
            }
        }

        @Override
        public void cleanup(final Reducer<LongWritable, HadoopVertex, NullWritable, HadoopVertex>.Context context) throws IOException, InterruptedException {
            this.graph.commit();
            this.graph.shutdown();
        }
    }
}
