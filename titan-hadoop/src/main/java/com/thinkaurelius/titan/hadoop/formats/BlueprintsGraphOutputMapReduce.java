package com.thinkaurelius.titan.hadoop.formats;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.hadoop.HadoopEdge;
import com.thinkaurelius.titan.hadoop.HadoopGraph;
import com.thinkaurelius.titan.hadoop.HadoopVertex;
import com.thinkaurelius.titan.hadoop.Holder;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.thinkaurelius.titan.hadoop.formats.titan.TitanOutputFormat;
import com.thinkaurelius.titan.hadoop.formats.titan.util.ConfigurationUtil;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;

import groovy.lang.MissingMethodException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import javax.script.Bindings;
import javax.script.ScriptException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * BlueprintsGraphOutputMapReduce will write a [NullWritable, HadoopVertex] stream to a Blueprints-enabled graph.
 * This is useful for bulk loading a Faunus graph into a Blueprints graph.
 * Graph writing happens in two distinction phase.
 * During the Map phase, all the vertices of the graph are written.
 * During the Reduce phase, all the edges of the graph are written.
 * Each stage is embarrassingly parallel with Map-to-Reduce communication only used to communicate generated vertex ids.
 * The output of the Reduce phase is a degenerate graph and is not considered viable for consumption.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BlueprintsGraphOutputMapReduce {

    public enum Counters {
        VERTICES_RETRIEVED,
        VERTICES_WRITTEN,
        VERTEX_PROPERTIES_WRITTEN,
        EDGES_WRITTEN,
        EDGE_PROPERTIES_WRITTEN,
        NULL_VERTEX_EDGES_IGNORED,
        NULL_VERTICES_IGNORED,
        SUCCESSFUL_TRANSACTIONS,
        FAILED_TRANSACTIONS
    }

    private static final String GET_OR_CREATE_VERTEX = "getOrCreateVertex(faunusVertex,graph,mapContext)";
    private static final String GET_OR_CREATE_EDGE = "getOrCreateEdge(faunusEdge,blueprintsOutVertex,blueprintsInVertex,graph,mapContext)";

    private static final String FAUNUS_VERTEX = "faunusVertex";
    private static final String FAUNUS_EDGE = "faunusEdge";
    private static final String BLUEPRINTS_OUT_VERTEX = "blueprintsOutVertex";
    private static final String BLUEPRINTS_IN_VERTEX = "blueprintsInVertex";
    private static final String GRAPH = "graph";
    private static final String MAP_CONTEXT = "mapContext";

    public static final String FAUNUS_GRAPH_OUTPUT_BLUEPRINTS_SCRIPT_FILE = "faunus.graph.output.blueprints.script-file";

    public static final Logger LOGGER = Logger.getLogger(BlueprintsGraphOutputMapReduce.class);
    // some random property that will 'never' be used by anyone
    public static final String BLUEPRINTS_ID = "_bId0192834";
    public static final String ID_MAP_KEY = "_iDMaPKeY";

    public static Graph generateGraph(final Configuration config) {
        final Class<? extends OutputFormat> format = config.getClass(HadoopGraph.FAUNUS_GRAPH_OUTPUT_FORMAT, OutputFormat.class, OutputFormat.class);
        if (TitanOutputFormat.class.isAssignableFrom(format)) {
            return TitanFactory.open(ConfigurationUtil.extractConfiguration(config, TitanOutputFormat.FAUNUS_GRAPH_OUTPUT_TITAN));
        } else {
            // TODO: this is where Rexster can come into play here
            throw new RuntimeException("The provide graph output format is not supported: " + format.getName());
        }
    }

    public static Configuration createConfiguration() {
        final Configuration configuration = new EmptyConfiguration();
        configuration.setBoolean("mapred.map.tasks.speculative.execution", false);
        configuration.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        return configuration;
    }

    ////////////// MAP/REDUCE WORK FROM HERE ON OUT

    // WRITE ALL THE VERTICES AND THEIR PROPERTIES
    public static class VertexMap extends Mapper<NullWritable, HadoopVertex, LongWritable, Holder<HadoopVertex>> {

        static GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        static boolean firstRead = true;
        Graph graph;
        boolean loadingFromScratch;

        private final Holder<HadoopVertex> vertexHolder = new Holder<HadoopVertex>();
        private final LongWritable longWritable = new LongWritable();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.graph = BlueprintsGraphOutputMapReduce.generateGraph(context.getConfiguration());
            final String file = context.getConfiguration().get(FAUNUS_GRAPH_OUTPUT_BLUEPRINTS_SCRIPT_FILE, null);
            if (null != file && firstRead) {
                final FileSystem fs = FileSystem.get(context.getConfiguration());
                try {
                    engine = new GremlinGroovyScriptEngine();
                    engine.eval(new InputStreamReader(fs.open(new Path(file))));
                    try {
                        engine.eval("getOrCreateVertex(null,null,null)");
                    } catch (ScriptException se) {
                        if (se.getCause().getCause() instanceof MissingMethodException)
                            engine = null;
                    }
                } catch (Exception e) {
                    throw new IOException(e.getMessage());
                }
                firstRead = false;
            }
            LOGGER.setLevel(Level.INFO);
        }


        @Override
        public void map(final NullWritable key, final HadoopVertex value, final Mapper<NullWritable, HadoopVertex, LongWritable, Holder<HadoopVertex>>.Context context) throws IOException, InterruptedException {
            try {
                // Read (and/or Write) HadoopVertex (and respective properties) to Blueprints Graph
                // Attempt to use the ID provided by Faunus
                final Vertex blueprintsVertex = this.getOrCreateVertex(value, context);

                // Propagate shell vertices with Blueprints ids
                final HadoopVertex shellVertex = new HadoopVertex(context.getConfiguration(), value.getIdAsLong());
                shellVertex.setProperty(BLUEPRINTS_ID, blueprintsVertex.getId());
                // TODO: Might need to be OUT for the sake of unidirectional edges in Titan
                for (final Edge faunusEdge : value.getEdges(IN)) {
                    this.longWritable.set((Long) faunusEdge.getVertex(OUT).getId());
                    context.write(this.longWritable, this.vertexHolder.set('s', shellVertex));
                }

                this.longWritable.set(value.getIdAsLong());
                value.getProperties().clear();  // no longer needed in reduce phase
                value.setProperty(BLUEPRINTS_ID, blueprintsVertex.getId()); // need this for id resolution in reduce phase
                value.removeEdges(Tokens.Action.DROP, IN); // no longer needed in reduce phase
                context.write(this.longWritable, this.vertexHolder.set('v', value));
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
                    LOGGER.error("Could not commit transaction during Map.cleanup():", e);
                    ((TransactionalGraph) this.graph).rollback();
                    context.getCounter(Counters.FAILED_TRANSACTIONS).increment(1l);
                    throw new IOException(e.getMessage(), e);
                }
            }
            this.graph.shutdown();
        }

        public Vertex getOrCreateVertex(final HadoopVertex faunusVertex, final Mapper<NullWritable, HadoopVertex, LongWritable, Holder<HadoopVertex>>.Context context) throws InterruptedException {
            final Vertex blueprintsVertex;
            if (this.loadingFromScratch) {
                blueprintsVertex = this.graph.addVertex(faunusVertex.getIdAsLong());
                context.getCounter(Counters.VERTICES_WRITTEN).increment(1l);
                for (final String property : faunusVertex.getPropertyKeys()) {
                    blueprintsVertex.setProperty(property, faunusVertex.getProperty(property));
                    context.getCounter(Counters.VERTEX_PROPERTIES_WRITTEN).increment(1l);
                }
            } else {
                try {
                    final Bindings bindings = engine.createBindings();
                    bindings.put(FAUNUS_VERTEX, faunusVertex);
                    bindings.put(GRAPH, this.graph);
                    bindings.put(MAP_CONTEXT, context);
                    blueprintsVertex = (Vertex) engine.eval(GET_OR_CREATE_VERTEX, bindings);
                } catch (Exception e) {
                    throw new InterruptedException(e.getMessage());
                }
            }
            return blueprintsVertex;
        }
    }

    public static class Reduce extends Reducer<LongWritable, Holder<HadoopVertex>, NullWritable, HadoopVertex> {

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder<HadoopVertex>> values, final Reducer<LongWritable, Holder<HadoopVertex>, NullWritable, HadoopVertex>.Context context) throws IOException, InterruptedException {

            HadoopVertex faunusVertex = null;
            // generate a map of the faunus id with the blueprints id for all shell vertices (vertices incoming adjacent)
            final java.util.Map<Long, Object> faunusBlueprintsIdMap = new HashMap<Long, Object>();
            for (final Holder<HadoopVertex> holder : values) {
                if (holder.getTag() == 's') {
                    faunusBlueprintsIdMap.put(holder.get().getIdAsLong(), holder.get().getProperty(BLUEPRINTS_ID));
                } else {
                    final HadoopVertex toClone = holder.get();
                    faunusVertex = new HadoopVertex(context.getConfiguration(), toClone.getIdAsLong());
                    faunusVertex.setProperty(BLUEPRINTS_ID, toClone.getProperty(BLUEPRINTS_ID));
                    faunusVertex.addEdges(OUT, toClone);
                }
            }
            if (null != faunusVertex) {
                faunusVertex.setProperty(ID_MAP_KEY, faunusBlueprintsIdMap);
                context.write(NullWritable.get(), faunusVertex);
            } else {
                LOGGER.warn("No source vertex: faunusVertex[" + key.get() + "]");
                context.getCounter(Counters.NULL_VERTICES_IGNORED).increment(1l);
            }
        }
    }

    // WRITE ALL THE EDGES CONNECTING THE VERTICES
    public static class EdgeMap extends Mapper<NullWritable, HadoopVertex, NullWritable, HadoopVertex> {

        static GremlinGroovyScriptEngine engine = null;
        static boolean firstRead = true;
        Graph graph;

        private static final HadoopVertex DEAD_FAUNUS_VERTEX = new HadoopVertex();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.graph = BlueprintsGraphOutputMapReduce.generateGraph(context.getConfiguration());
            final String file = context.getConfiguration().get(FAUNUS_GRAPH_OUTPUT_BLUEPRINTS_SCRIPT_FILE, null);
            if (null != file && firstRead) {
                final FileSystem fs = FileSystem.get(context.getConfiguration());
                try {
                    engine = new GremlinGroovyScriptEngine();
                    engine.eval(new InputStreamReader(fs.open(new Path(file))));
                    try {
                        engine.eval("getOrCreateEdge(null,null,null,null,null)");
                    } catch (ScriptException se) {
                        if (se.getCause().getCause() instanceof MissingMethodException)
                            engine = null;
                    }
                } catch (Exception e) {
                    throw new IOException(e.getMessage());
                }
                firstRead = false;
            }
            LOGGER.setLevel(Level.INFO);
        }

        @Override
        public void map(final NullWritable key, final HadoopVertex value, final Mapper<NullWritable, HadoopVertex, NullWritable, HadoopVertex>.Context context) throws IOException, InterruptedException {
            try {
                final java.util.Map<Long, Object> faunusBlueprintsIdMap = value.getProperty(ID_MAP_KEY);
                final Object blueprintsId = value.getProperty(BLUEPRINTS_ID);
                Vertex blueprintsVertex = null;
                if (null != blueprintsId)
                    blueprintsVertex = this.graph.getVertex(blueprintsId);
                // this means that an adjacent vertex to this vertex wasn't created
                if (null != blueprintsVertex) {
                    for (final Edge faunusEdge : value.getEdges(OUT)) {
                        final Object otherId = faunusBlueprintsIdMap.get(faunusEdge.getVertex(IN).getId());
                        Vertex otherVertex = null;
                        if (null != otherId)
                            otherVertex = this.graph.getVertex(otherId);
                        if (null != otherVertex) {
                            this.getOrCreateEdge((HadoopEdge) faunusEdge, blueprintsVertex, otherVertex, context);
                        } else {
                            LOGGER.warn("No target vertex: faunusVertex[" + faunusEdge.getVertex(IN).getId() + "] blueprintsVertex[" + otherId + "]");
                            context.getCounter(Counters.NULL_VERTEX_EDGES_IGNORED).increment(1l);
                        }
                    }
                } else {
                    LOGGER.warn("No source vertex: faunusVertex[" + NullWritable.get() + "] blueprintsVertex[" + blueprintsId + "]");
                    context.getCounter(Counters.NULL_VERTICES_IGNORED).increment(1l);
                }
                // the emitted vertex is not complete -- assuming this is the end of the stage and vertex is dead
                context.write(NullWritable.get(), DEAD_FAUNUS_VERTEX);
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
                    LOGGER.error("Could not commit transaction during Reduce.cleanup():", e);
                    ((TransactionalGraph) this.graph).rollback();
                    context.getCounter(Counters.FAILED_TRANSACTIONS).increment(1l);
                    throw new IOException(e.getMessage(), e);
                }
            }
            this.graph.shutdown();
        }

        public Edge getOrCreateEdge(final HadoopEdge hadoopEdge, final Vertex blueprintsOutVertex, final Vertex blueprintsInVertex, final Mapper<NullWritable, HadoopVertex, NullWritable, HadoopVertex>.Context context) throws InterruptedException {
            final Edge blueprintsEdge;
            if (null == engine) {
                blueprintsEdge = this.graph.addEdge(null, blueprintsOutVertex, blueprintsInVertex, hadoopEdge.getLabel());
                context.getCounter(Counters.EDGES_WRITTEN).increment(1l);
                for (final String property : hadoopEdge.getPropertyKeys()) {
                    blueprintsEdge.setProperty(property, hadoopEdge.getProperty(property));
                    context.getCounter(Counters.EDGE_PROPERTIES_WRITTEN).increment(1l);
                }
            } else {
                try {
                    final Bindings bindings = engine.createBindings();
                    bindings.put(FAUNUS_EDGE, hadoopEdge);
                    bindings.put(BLUEPRINTS_OUT_VERTEX, blueprintsOutVertex);
                    bindings.put(BLUEPRINTS_IN_VERTEX, blueprintsInVertex);
                    bindings.put(GRAPH, this.graph);
                    bindings.put(MAP_CONTEXT, context);
                    blueprintsEdge = (Edge) engine.eval(GET_OR_CREATE_EDGE, bindings);
                } catch (Exception e) {
                    throw new InterruptedException(e.getMessage());
                }
            }
            return blueprintsEdge;
        }
    }

}
