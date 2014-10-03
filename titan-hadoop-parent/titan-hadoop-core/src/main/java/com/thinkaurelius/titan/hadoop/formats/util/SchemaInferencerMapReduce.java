package com.thinkaurelius.titan.hadoop.formats.util;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.VertexLabel;
import com.thinkaurelius.titan.core.schema.DefaultSchemaMaker;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.graphdb.blueprints.BlueprintsDefaultSchemaMaker;
import com.thinkaurelius.titan.graphdb.types.system.BaseVertexLabel;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SchemaInferencerMapReduce {

    public enum Counters {
        EDGE_LABELS_CREATED,
        VERTEX_LABELS_CREATED,
        PROPERTY_KEYS_CREATED
    }

    private static final long funnyLong = Long.MAX_VALUE; // TODO delete this, move it out-of-band
    private static final LongWritable funnyKey = new LongWritable(funnyLong);
    private static final Logger log = LoggerFactory.getLogger(SchemaInferencerMapReduce.class);

    public static class Map extends Mapper<NullWritable, FaunusVertex, LongWritable, FaunusVertex> {

        private FaunusVertex funnyVertex;
        private Configuration faunusConf;
        private final LongWritable longWritable = new LongWritable();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            faunusConf = ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getContextConfiguration(context));
            this.funnyVertex = new FaunusVertex(faunusConf, funnyLong);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            //Vertex labels
            VertexLabel vl = value.getVertexLabel();
            if (vl!= BaseVertexLabel.DEFAULT_VERTEXLABEL) {
                this.funnyVertex.setProperty("v"+vl.getName(),String.class.getName());
            }

            //Vertex keys
            for (final String property : value.getPropertyKeys()) {
                this.funnyVertex.setProperty("k" + property, Object.class.getName());
                // TODO: Automated type inference
            }

            //Edge Labels
            for (final Edge edge : value.getEdges(Direction.OUT)) {
                this.funnyVertex.setProperty("l" + edge.getLabel(), String.class.getName());
                //Edge keys
                for (final String property : edge.getPropertyKeys()) {
                    this.funnyVertex.setProperty("k" + property, Object.class.getName());
                }
            }

            this.longWritable.set(value.getLongId());
            context.write(this.longWritable, value);
        }

        @Override
        public void cleanup(final Mapper<NullWritable, FaunusVertex, LongWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            context.write(funnyKey, this.funnyVertex);
        }
    }

    public static class Reduce extends org.apache.hadoop.mapreduce.Reducer<LongWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private TitanGraph graph;
        private ModifiableHadoopConfiguration faunusConf;
        private TitanTransaction tx;

        @Override
        public void setup(final Reduce.Context context) throws IOException, InterruptedException {
            faunusConf = ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getContextConfiguration(context));
            graph = TitanGraphOutputMapReduce.generateGraph(faunusConf);
            tx = graph.buildTransaction().disableBatchLoading().start();

            log.debug("Dumping configuration");
            for (java.util.Map.Entry<String, String> ent : faunusConf.getHadoopConfiguration()) {
                log.debug("[SchemaInferencerMRConfig] {}={}", ent.getKey(), ent.getValue());
            }
            log.debug("Done dumping configuration");

            log.debug("Dumping credentials");
            for (Token token : context.getCredentials().getAllTokens()) {
                log.debug("[Credentials] kind={} ident={} token={}", token.getKind(), token.getIdentifier(), token);
            }
            log.debug("Done dumping credentials");
        }

        @Override
        public void reduce(final LongWritable key, final Iterable<FaunusVertex> value, final Reducer<LongWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            if (key.get() == funnyLong) {
                final DefaultSchemaMaker typeMaker = BlueprintsDefaultSchemaMaker.INSTANCE;
                for (final FaunusVertex vertex : value) {
                    for (final String property : vertex.getPropertyKeys()) {
                        final char type = property.charAt(0);
                        final String typeName = property.substring(1);
                        if ( ((type=='k' || type=='l') && tx.getRelationType(typeName)!=null)
                                || (type=='v' && tx.containsVertexLabel(typeName))) continue;

                        if (type=='k') {
                            // TODO: Automated type inference
                            // typeMaker.makeKey(property2, tx.makeType().dataType(Class.forName(vertex.getProperty(property).toString())));
                            typeMaker.makePropertyKey(tx.makePropertyKey(typeName));
                            DEFAULT_COMPAT.incrementContextCounter(context, Counters.PROPERTY_KEYS_CREATED, 1L);
                        } else if (type=='l') {
                            //typeMaker.makeLabel(property2, tx.makeType());
                            typeMaker.makeEdgeLabel(tx.makeEdgeLabel(typeName));
                            DEFAULT_COMPAT.incrementContextCounter(context, Counters.EDGE_LABELS_CREATED, 1L);
                        } else if (type=='v') {
                            typeMaker.makeVertexLabel(tx.makeVertexLabel(typeName));
                            DEFAULT_COMPAT.incrementContextCounter(context, Counters.VERTEX_LABELS_CREATED, 1L);

                        } else throw new IllegalArgumentException("Unexpected type: " + type);
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
            tx.commit();
            graph.shutdown();
        }
    }
}
