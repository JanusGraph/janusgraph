package com.thinkaurelius.titan.hadoop.formats.titan;

import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.hadoop.HadoopEdge;
import com.thinkaurelius.titan.hadoop.HadoopGraph;
import com.thinkaurelius.titan.hadoop.HadoopProperty;
import com.thinkaurelius.titan.hadoop.HadoopVertex;
import com.thinkaurelius.titan.hadoop.Holder;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.thinkaurelius.titan.hadoop.formats.titan.util.ConfigurationUtil;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanGraphOutputMapReduce {

    public enum Counters {
        VERTICES_ADDED,
        VERTICES_REMOVED,
        VERTEX_PROPERTIES_ADDED,
        VERTEX_PROPERTIES_REMOVED,
        EDGES_ADDED,
        EDGES_REMOVED,
        EDGE_PROPERTIES_ADDED,
        EDGE_PROPERTIES_REMOVED,
        NULL_VERTEX_EDGES_IGNORED,
        NULL_VERTICES_IGNORED,
        NULL_EDGES_IGNORED,
        SUCCESSFUL_TRANSACTIONS,
        FAILED_TRANSACTIONS
    }

    public static final Logger LOGGER = Logger.getLogger(TitanGraphOutputMapReduce.class);
    // some random property that will 'never' be used by anyone
    public static final String TITAN_ID = "_bId0192834";
    public static final String ID_MAP_KEY = "_iDMaPKeY";

    /*private static final String GET_OR_CREATE_VERTEX = "getCreateOrDeleteVertex(faunusVertex,graph,mapContext)";
    private static final String FAUNUS_VERTEX = "faunusVertex";
    private static final String GRAPH = "graph";
    private static final String MAP_CONTEXT = "mapContext";

    public static final String TITAN_HADOOP_GRAPH_OUTPUT_BLUEPRINTS_SCRIPT_FILE = "faunus.graph.output.blueprints.script-file";*/

    public static Graph generateGraph(final Configuration configuration) {
        final Class<? extends OutputFormat> format = configuration.getClass(HadoopGraph.TITAN_HADOOP_GRAPH_OUTPUT_FORMAT, OutputFormat.class, OutputFormat.class);
        if (TitanOutputFormat.class.isAssignableFrom(format)) {
            return TitanFactory.open(ConfigurationUtil.extractConfiguration(configuration, TitanOutputFormat.TITAN_HADOOP_GRAPH_OUTPUT_TITAN));
        } else {
            throw new RuntimeException("The provide graph output format is not a supported TitanOutputFormat: " + format.getName());
        }
    }

    public static Configuration createConfiguration() {
        final Configuration configuration = new EmptyConfiguration();
        configuration.setBoolean("mapred.map.tasks.speculative.execution", false);
        configuration.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        return configuration;
    }

    // WRITE ALL THE VERTICES AND THEIR PROPERTIES
    public static class VertexMap extends Mapper<NullWritable, HadoopVertex, LongWritable, Holder<HadoopVertex>> {

        Graph graph;
        boolean trackState;

        private final Holder<HadoopVertex> vertexHolder = new Holder<HadoopVertex>();
        private final LongWritable longWritable = new LongWritable();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.graph = TitanGraphOutputMapReduce.generateGraph(context.getConfiguration());
            this.trackState = context.getConfiguration().getBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_STATE, false);
            LOGGER.setLevel(Level.INFO);
        }

        @Override
        public void map(final NullWritable key, final HadoopVertex value, final Mapper<NullWritable, HadoopVertex, LongWritable, Holder<HadoopVertex>>.Context context) throws IOException, InterruptedException {
            try {
                final Vertex titanVertex = this.getCreateOrDeleteVertex(value, context);
                if (null != titanVertex) { // the vertex was state != deleted (if it was we know incident edges are deleted too)
                    // Propagate shell vertices with Blueprints ids
                    final HadoopVertex shellVertex = new HadoopVertex(context.getConfiguration(), value.getIdAsLong());
                    shellVertex.setProperty(TITAN_ID, titanVertex.getId());
                    for (final Edge hadoopEdge : value.getEdges(OUT)) {
                        this.longWritable.set((Long) hadoopEdge.getVertex(IN).getId());
                        context.write(this.longWritable, this.vertexHolder.set('s', shellVertex));
                    }

                    this.longWritable.set(value.getIdAsLong());
                    value.getPropertiesWithState().clear();  // no longer needed in reduce phase
                    value.setProperty(TITAN_ID, titanVertex.getId()); // need this for id resolution in edge-map phase
                    value.removeEdges(Tokens.Action.DROP, OUT); // no longer needed in reduce phase
                    context.write(this.longWritable, this.vertexHolder.set('v', value));
                }
            } catch (final Exception e) {
                if (this.graph instanceof TransactionalGraph) {
                    ((TransactionalGraph) this.graph).rollback();
                    context.getCounter(Counters.FAILED_TRANSACTIONS).increment(1l);
                }
                throw new IOException(e.getMessage(), e);
            }

        }

        @Override
        public void cleanup(final Mapper<NullWritable, HadoopVertex, LongWritable, Holder<HadoopVertex>>.Context context) throws IOException, InterruptedException {
            if (this.graph instanceof TransactionalGraph) {
                try {
                    ((TransactionalGraph) this.graph).commit();
                    context.getCounter(Counters.SUCCESSFUL_TRANSACTIONS).increment(1l);
                } catch (Exception e) {
                    LOGGER.error("Could not commit transaction during Map.cleanup(): ", e);
                    ((TransactionalGraph) this.graph).rollback();
                    context.getCounter(Counters.FAILED_TRANSACTIONS).increment(1l);
                    throw new IOException(e.getMessage(), e);
                }
            }
            this.graph.shutdown();
        }

        public Vertex getCreateOrDeleteVertex(final HadoopVertex hadoopVertex, final Mapper<NullWritable, HadoopVertex, LongWritable, Holder<HadoopVertex>>.Context context) throws InterruptedException {
            if (this.trackState && hadoopVertex.isDeleted()) {
                final Vertex titanVertex = this.graph.getVertex(hadoopVertex.getId());
                if (null == titanVertex)
                    context.getCounter(Counters.NULL_VERTICES_IGNORED).increment(1l);
                else {
                    titanVertex.remove();
                    context.getCounter(Counters.VERTICES_REMOVED).increment(1l);
                }
                return null;
            } else if (this.trackState && hadoopVertex.isLoaded()) {
                final TitanVertex titanVertex = (TitanVertex) this.graph.getVertex(hadoopVertex.getId());
                if (null == titanVertex)
                    context.getCounter(Counters.NULL_VERTICES_IGNORED).increment(1l);
                else {
                    for (final HadoopProperty hadoopProperty : hadoopVertex.getPropertiesWithState()) {
                        if (hadoopProperty.isNew()) {
                            titanVertex.addProperty(hadoopProperty.getName(), hadoopProperty.getValue());
                            context.getCounter(Counters.VERTEX_PROPERTIES_ADDED).increment(1l);
                        } else if (hadoopProperty.isDeleted()) {
                            for (final TitanProperty titanProperty : titanVertex.getProperties(hadoopProperty.getName())) {
                                if (titanProperty.getID() == hadoopProperty.getIdAsLong()) {
                                    titanProperty.remove();
                                    context.getCounter(Counters.VERTEX_PROPERTIES_REMOVED).increment(1l);
                                }
                            }
                        }
                    }
                }
                return titanVertex;
            } else {   // state == new || !trackState
                final TitanVertex titanVertex = (TitanVertex) this.graph.addVertex(hadoopVertex.getId());
                context.getCounter(Counters.VERTICES_ADDED).increment(1l);
                for (final HadoopProperty property : hadoopVertex.getProperties()) {
                    titanVertex.addProperty(property.getName(), property.getValue());
                    context.getCounter(Counters.VERTEX_PROPERTIES_ADDED).increment(1l);
                }
                return titanVertex;
            }
        }
    }

    public static class Reduce extends Reducer<LongWritable, Holder<HadoopVertex>, NullWritable, HadoopVertex> {

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder<HadoopVertex>> values, final Reducer<LongWritable, Holder<HadoopVertex>, NullWritable, HadoopVertex>.Context context) throws IOException, InterruptedException {
            HadoopVertex hadoopVertex = null;
            // generate a map of the Titan/Hadoop id with the Titan id for all shell vertices (vertices incoming adjacent)
            final java.util.Map<Long, Object> idMap = new HashMap<Long, Object>();
            for (final Holder<HadoopVertex> holder : values) {
                if (holder.getTag() == 's') {
                    idMap.put(holder.get().getIdAsLong(), holder.get().getProperty(TITAN_ID));
                } else {
                    hadoopVertex = holder.get();
                }
            }
            if (null != hadoopVertex) {
                hadoopVertex.setProperty(ID_MAP_KEY, idMap);
                context.write(NullWritable.get(), hadoopVertex);
            } else {
                LOGGER.warn("No source vertex: hadoopVertex[" + key.get() + "]");
                context.getCounter(Counters.NULL_VERTICES_IGNORED).increment(1l);
            }
        }
    }

    // WRITE ALL THE EDGES CONNECTING THE VERTICES
    public static class EdgeMap extends Mapper<NullWritable, HadoopVertex, NullWritable, HadoopVertex> {

        Graph graph;
        boolean trackState;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.graph = TitanGraphOutputMapReduce.generateGraph(context.getConfiguration());
            this.trackState = context.getConfiguration().getBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_STATE, false);
            LOGGER.setLevel(Level.INFO);
        }

        @Override
        public void map(final NullWritable key, final HadoopVertex value, final Mapper<NullWritable, HadoopVertex, NullWritable, HadoopVertex>.Context context) throws IOException, InterruptedException {
            try {
                for (final HadoopEdge edge : value.getEdgesWithState(IN)) {
                    this.getCreateOrDeleteEdge(value, edge, context);
                }
            } catch (final Exception e) {
                if (this.graph instanceof TransactionalGraph) {
                    ((TransactionalGraph) this.graph).rollback();
                    context.getCounter(Counters.FAILED_TRANSACTIONS).increment(1l);
                }
                throw new IOException(e.getMessage(), e);
            }
        }

        @Override
        public void cleanup(final Mapper<NullWritable, HadoopVertex, NullWritable, HadoopVertex>.Context context) throws IOException, InterruptedException {
            if (this.graph instanceof TransactionalGraph) {
                try {
                    ((TransactionalGraph) this.graph).commit();
                    context.getCounter(Counters.SUCCESSFUL_TRANSACTIONS).increment(1l);
                } catch (Exception e) {
                    LOGGER.error("Could not commit transaction during Reduce.cleanup(): ", e);
                    ((TransactionalGraph) this.graph).rollback();
                    context.getCounter(Counters.FAILED_TRANSACTIONS).increment(1l);
                    throw new IOException(e.getMessage(), e);
                }
            }
            this.graph.shutdown();
        }

        public Edge getCreateOrDeleteEdge(final HadoopVertex hadoopVertex, final HadoopEdge hadoopEdge, final Mapper<NullWritable, HadoopVertex, NullWritable, HadoopVertex>.Context context) throws InterruptedException {
            final TitanVertex titanVertex = (TitanVertex) this.graph.getVertex(hadoopVertex.getProperty(TITAN_ID));
            final java.util.Map<Long, Object> idMap = hadoopVertex.getProperty(ID_MAP_KEY);
            final boolean isModified = hadoopEdge.isModified();
            if (this.trackState && (isModified || hadoopEdge.isDeleted())) {
                final TitanEdge titanEdge = this.getIncident(titanVertex, hadoopEdge, idMap.get(hadoopEdge.getVertexId(OUT)));
                if (null == titanEdge)
                    context.getCounter(Counters.NULL_EDGES_IGNORED).increment(1l);
                else {
                    titanEdge.remove();
                    context.getCounter(Counters.EDGES_REMOVED).increment(1l);
                }
            }
            if (isModified || hadoopEdge.isNew()) {
                final TitanEdge titanEdge = (TitanEdge) this.graph.getVertex(idMap.get(hadoopEdge.getVertexId(OUT))).addEdge(hadoopEdge.getLabel(), titanVertex);
                context.getCounter(Counters.EDGES_ADDED).increment(1l);
                for (final HadoopProperty hadoopProperty : hadoopEdge.getProperties()) {
                    titanEdge.setProperty(hadoopProperty.getName(), hadoopProperty.getValue());
                    context.getCounter(Counters.EDGE_PROPERTIES_ADDED).increment(1l);
                }
                return titanEdge;
            } else {
                return null;
            }
        }

        private TitanEdge getIncident(final TitanVertex titanVertex, HadoopEdge hadoopEdge, final Object otherVertexId) {
            final VertexQuery query = (null == otherVertexId) ?   // the shell wasn't propagated because the vertex was deleted -- should we propagate shell?
                    titanVertex.query().direction(IN).labels(hadoopEdge.getLabel()) :
                    titanVertex.query().direction(IN).labels(hadoopEdge.getLabel()).adjacent((TitanVertex) this.graph.getVertex(otherVertexId));
            for (final HadoopProperty property : hadoopEdge.getPropertiesWithState()) {
                if (property.isLoaded()) {
                    query.has(property.getName(), property.getValue());
                }
            }

            for (final Edge edge : query.edges()) {
                if (((TitanEdge) edge).getID() == hadoopEdge.getIdAsLong()) {
                    return (TitanEdge) edge;
                }
            }
            return null;
        }
    }
}
