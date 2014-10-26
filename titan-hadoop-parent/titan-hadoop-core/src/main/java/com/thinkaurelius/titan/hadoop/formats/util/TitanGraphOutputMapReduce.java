package com.thinkaurelius.titan.hadoop.formats.util;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;
import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.OUTPUT_FORMAT;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.graphdb.types.system.BaseVertexLabel;
import com.thinkaurelius.titan.hadoop.*;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.tinkerpop.blueprints.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.OUTPUT_LOADER_SCRIPT_FILE;
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
        NULL_RELATIONS_IGNORED,
        SUCCESSFUL_TRANSACTIONS,
        FAILED_TRANSACTIONS,
    }

    public static final Logger LOGGER = LoggerFactory.getLogger(TitanGraphOutputMapReduce.class);

    // TODO move this out-of-band
    // some random property that will 'never' be used by anyone
    public static final String TITAN_ID = "_bId0192834";
    public static final String ID_MAP_KEY = "_iDMaPKeY";

    private static final String HADOOP_VERTEX = "hadoopVertex";
    private static final String HADOOP_EDGE = "hadoopEdge";
    private static final String TITAN_OUT_VERTEX = "titanOutVertex";
    private static final String TITAN_IN_VERTEX = "titanInVertex";
    private static final String GRAPH = "graph";
    private static final String MAP_CONTEXT = "mapContext";

    /*private static final String FAUNUS_VERTEX = "faunusVertex";
    private static final String GRAPH = "graph";
    private static final String MAP_CONTEXT = "mapContext"; */

    public static TitanGraph generateGraph(final ModifiableHadoopConfiguration titanConf) {
        final Class<? extends OutputFormat> format = titanConf.getClass(OUTPUT_FORMAT, OutputFormat.class, OutputFormat.class);
        if (TitanOutputFormat.class.isAssignableFrom(format)) {
            ModifiableConfiguration mc = titanConf.getOutputConf();
            boolean present = mc.has(AbstractCassandraStoreManager.CASSANDRA_KEYSPACE);
            LOGGER.trace("Keyspace in_config=" + present + " value=" + mc.get(AbstractCassandraStoreManager.CASSANDRA_KEYSPACE));
            return TitanFactory.open(mc);
        } else {
            throw new RuntimeException("The provide graph output format is not a supported TitanOutputFormat: " + format.getName());
        }
    }

    //UTILITY METHODS
    private static Object getValue(TitanRelation relation, TitanGraph graph) {
        if (relation.isProperty()) return ((TitanVertexProperty)relation).value();
        else return graph.v(((TitanEdge) relation).vertex(IN).longId());
    }

    // WRITE ALL THE VERTICES AND THEIR PROPERTIES
    public static class VertexMap extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>> {

        private TitanGraph graph;
        private boolean trackState;
        private ModifiableHadoopConfiguration faunusConf;
        private LoaderScriptWrapper loaderScript;

        private final Holder<FaunusVertex> vertexHolder = new Holder<FaunusVertex>();
        private final LongWritable longWritable = new LongWritable();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            faunusConf = ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getContextConfiguration(context));
            graph = TitanGraphOutputMapReduce.generateGraph(faunusConf);
            trackState = context.getConfiguration().getBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_STATE, false);

            // Check whether a script is defined in the config
            if (faunusConf.has(OUTPUT_LOADER_SCRIPT_FILE)) {
                Path scriptPath = new Path(faunusConf.get(OUTPUT_LOADER_SCRIPT_FILE));
                FileSystem scriptFS = FileSystem.get(DEFAULT_COMPAT.getJobContextConfiguration(context));
                loaderScript = new LoaderScriptWrapper(scriptFS, scriptPath);
            }
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>>.Context context) throws IOException, InterruptedException {
            try {
                final TitanVertex titanVertex = this.getCreateOrDeleteVertex(value, context);
                if (null != titanVertex) { // the vertex was state != deleted (if it was we know incident edges are deleted too)
                    // Propagate shell vertices with Titan ids
                    final FaunusVertex shellVertex = new FaunusVertex(faunusConf, value.longId());
                    shellVertex.property(TITAN_ID, titanVertex.longId());
                    for (final TitanEdge edge : value.query().direction(OUT).titanEdges()) {
                        if (!trackState || edge.isNew()) { //Only need to propagate ids for new edges
                            this.longWritable.set(edge.vertex(IN).longId());
                            context.write(this.longWritable, this.vertexHolder.set('s', shellVertex));
                        }
                    }

                    this.longWritable.set(value.longId());
//                    value.getPropertiesWithState().clear();  // no longer needed in reduce phase
                    value.property(TITAN_ID, titanVertex.longId()); // need this for id resolution in edge-map phase
//                    value.removeEdges(Tokens.Action.DROP, OUT); // no longer needed in reduce phase
                    context.write(this.longWritable, this.vertexHolder.set('v', value));
                }
            } catch (final Exception e) {
                graph.rollback();
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.FAILED_TRANSACTIONS, 1L);
                throw new IOException(e.getMessage(), e);
            }

        }

        @Override
        public void cleanup(final Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>>.Context context) throws IOException, InterruptedException {
            try {
                graph.commit();
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.SUCCESSFUL_TRANSACTIONS, 1L);
            } catch (Exception e) {
                LOGGER.error("Could not commit transaction during Map.cleanup(): ", e);
                graph.rollback();
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.FAILED_TRANSACTIONS, 1L);
                throw new IOException(e.getMessage(), e);
            }
            graph.shutdown();
        }

        public TitanVertex getCreateOrDeleteVertex(final FaunusVertex faunusVertex, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>>.Context context) throws InterruptedException {
            if (this.trackState && faunusVertex.isRemoved()) {
                final Vertex titanVertex = graph.v(faunusVertex.longId());
                if (null == titanVertex)
                    DEFAULT_COMPAT.incrementContextCounter(context, Counters.NULL_VERTICES_IGNORED, 1L);
                else {
                    titanVertex.remove();
                    DEFAULT_COMPAT.incrementContextCounter(context, Counters.VERTICES_REMOVED, 1L);
                }
                return null;
            } else {
                final TitanVertex titanVertex;
                if (faunusVertex.isNew()) {
                    // Vertex is new to this faunus run, but might already exist in Titan
                    titanVertex = getTitanVertex(faunusVertex, context);
                } else {
                    titanVertex = (TitanVertex) graph.v(faunusVertex.longId());
                    if (titanVertex==null) {
                        DEFAULT_COMPAT.incrementContextCounter(context, Counters.NULL_VERTICES_IGNORED, 1L);
                        return null;
                    }
                }
                if (faunusVertex.isNew() || faunusVertex.isModified()) {
                    //Synchronize properties
                    for (final TitanVertexProperty p : faunusVertex.query().queryAll().properties()) {
                        if (null != loaderScript && loaderScript.hasVPropMethod()) {
                            loaderScript.getVProp(p, titanVertex, graph, context);
                        } else {
                            getCreateOrDeleteRelation(graph, trackState, OUT, faunusVertex, titanVertex,
                                    (StandardFaunusVertexProperty) p, context);
                        }
                    }
                }
                return titanVertex;
            }
        }

        private TitanVertex getTitanVertex(FaunusVertex faunusVertex, Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>>.Context context) {
            if (null != loaderScript && loaderScript.hasVertexMethod()) {
                return loaderScript.getVertex(faunusVertex, graph, context);
            } else {
                VertexLabel titanLabel = BaseVertexLabel.DEFAULT_VERTEXLABEL;
                FaunusVertexLabel faunusLabel = faunusVertex.vertexLabel();
                if (!faunusLabel.isDefault()) titanLabel = graph.getOrCreateVertexLabel(faunusLabel.name());
                TitanVertex tv = graph.addVertexWithLabel(titanLabel);
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.VERTICES_ADDED, 1L);
                return tv;
            }
        }

    }


    private static TitanRelation getCreateOrDeleteRelation(final TitanGraph graph, final boolean trackState, final Direction dir,
                                             final FaunusVertex faunusVertex, final TitanVertex titanVertex,
                                             final StandardFaunusRelation faunusRelation, final Mapper.Context context) {
        assert dir==IN || dir==OUT;

        final TitanRelation titanRelation;
        if (trackState && (faunusRelation.isModified() || faunusRelation.isRemoved())) { //Modify existing
            Map<Long, Long> idMap = getIdMap(faunusVertex);
            titanRelation = getIncidentRelation(graph, dir, titanVertex, faunusRelation,
                    faunusRelation.isEdge()?idMap.get(((FaunusEdge)faunusRelation).getVertexId(dir.opposite())):null);
            if (null == titanRelation) {
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.NULL_RELATIONS_IGNORED, 1L);
                return null;
            } else if (faunusRelation.isRemoved()) {
                titanRelation.remove();
                DEFAULT_COMPAT.incrementContextCounter(context,
                        faunusRelation.isEdge() ? Counters.EDGES_REMOVED : Counters.VERTEX_PROPERTIES_REMOVED, 1L);
                return null;
            }
        } else if (trackState && faunusRelation.isLoaded()) {
            return null;
        } else { //Create new
            assert faunusRelation.isNew();
            if (faunusRelation.isEdge()) {
                StandardFaunusEdge faunusEdge = (StandardFaunusEdge)faunusRelation;
                TitanVertex otherVertex = getOtherTitanVertex(faunusVertex, faunusEdge, dir.opposite(), graph);
                if (dir==IN) {
                    titanRelation = otherVertex.addEdge(faunusEdge.getLabel(), titanVertex);
                } else {
                    titanRelation = titanVertex.addEdge(faunusEdge.getLabel(), otherVertex);
                }
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.EDGES_ADDED, 1L);
            } else {
                StandardFaunusVertexProperty faunusProperty = (StandardFaunusVertexProperty)faunusRelation;
                assert dir==OUT;
                titanRelation = titanVertex.property(faunusProperty.getTypeName(), faunusProperty.value());
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.VERTEX_PROPERTIES_ADDED, 1L);
            }
        }

        synchronizeRelationProperties(graph, faunusRelation, titanRelation, context);

        return titanRelation;
    }

    private static TitanRelation synchronizeRelationProperties(final TitanGraph graph,
                                                               final StandardFaunusRelation faunusRelation,
                                                               final TitanRelation titanRelation,
                                                               final Mapper.Context context) {
        if (faunusRelation.isModified()  || faunusRelation.isNew()) { //Synchronize incident properties + unidirected edges
            for (TitanRelation faunusProp : faunusRelation.query().queryAll().relations()) {
                if (faunusProp.isRemoved()) {
                    titanRelation.property(faunusProp.getType().name()).remove();
                    DEFAULT_COMPAT.incrementContextCounter(context, Counters.EDGE_PROPERTIES_REMOVED, 1L);
                }
            }
            for (TitanRelation faunusProp : faunusRelation.query().queryAll().relations()) {
                if (faunusProp.isNew()) {
                    Object value;
                    if (faunusProp.isProperty()) {
                        value = ((FaunusVertexProperty)faunusProp).value();
                    } else {
                        //TODO: ensure that the adjacent vertex has been previous assigned an id since ids don't propagate along unidirected edges
                        value = graph.v(((FaunusEdge) faunusProp).getVertexId(IN));
                    }
                    titanRelation.property(faunusProp.getType().name(), value);
                    DEFAULT_COMPAT.incrementContextCounter(context, Counters.EDGE_PROPERTIES_ADDED, 1L);
                }
            }

        }

        return titanRelation;
    }

    private static TitanVertex getOtherTitanVertex(final FaunusVertex faunusVertex, final FaunusEdge faunusEdge, final Direction otherDir, final TitanGraph graph) {
        Map<Long, Long> idMap = getIdMap(faunusVertex);
        Long othervertexid = faunusEdge.getVertexId(otherDir);
        if (null != idMap && idMap.containsKey(othervertexid))
            othervertexid = idMap.get(othervertexid);
        TitanVertex otherVertex = graph.v(othervertexid);
        //TODO: check that other vertex has valid id assignment for unidirected edges
        return otherVertex;
    }

    private static Map<Long, Long> getIdMap(final FaunusVertex faunusVertex) {
        Map<Long, Long> idMap = faunusVertex.value(ID_MAP_KEY);
        if (null == idMap)
            idMap = ImmutableMap.of();
        return idMap;
    }

    private static TitanRelation getIncidentRelation(final TitanGraph graph, final Direction dir,
                                         final TitanVertex titanVertex, final StandardFaunusRelation faunusRelation, Long otherTitanVertexId) {
        TitanVertexQuery qb = titanVertex.query().direction(dir).types(graph.getRelationType(faunusRelation.getTypeName()));
        if (faunusRelation.isEdge()) {
            TitanVertex otherVertex;
            if (otherTitanVertexId!=null) {
                otherVertex = (TitanVertex)graph.v(otherTitanVertexId);
            } else {
                StandardFaunusEdge edge = (StandardFaunusEdge)faunusRelation;
                otherVertex = (TitanVertex) graph.v(edge.getVertexId(dir.opposite()));
            }
            if (otherVertex!=null) qb.adjacent(otherVertex);
            else return null;
        }
//        qb.has(ImplicitKey.TITANID.getName(), Cmp.EQUAL, faunusRelation.getLongId()); TODO: must check for multiplicity constraints
        TitanRelation titanRelation = (TitanRelation)Iterables.getFirst(Iterables.filter(faunusRelation.isEdge()?qb.titanEdges():qb.properties(),new Predicate<TitanRelation>() {
            @Override
            public boolean apply(@Nullable TitanRelation rel) {
                return rel.longId()==faunusRelation.longId();
            }
        }),null);
        assert titanRelation==null || titanRelation.longId()==faunusRelation.longId();
        return titanRelation;
    }

    //MAPS FAUNUS VERTEXIDs to TITAN VERTEXIDs
    public static class Reduce extends Reducer<LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex> {

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder<FaunusVertex>> values, final Reducer<LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            FaunusVertex faunusVertex = null;
            // generate a map of the Titan/Hadoop id with the Titan id for all shell vertices (vertices incoming adjacent)
            final java.util.Map<Long, Object> idMap = new HashMap<Long, Object>();
            for (final Holder<FaunusVertex> holder : values) {
                if (holder.getTag() == 's') {
                    idMap.put(holder.get().longId(), holder.get().value(TITAN_ID));
                } else {
                    faunusVertex = holder.get();
                }
            }
            if (null != faunusVertex) {
                faunusVertex.property(ID_MAP_KEY, idMap);
                context.write(NullWritable.get(), faunusVertex);
            } else {
                LOGGER.warn("No source vertex: hadoopVertex[" + key.get() + "]");
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.NULL_VERTICES_IGNORED, 1L);
            }
        }
    }

    // WRITE ALL THE EDGES CONNECTING THE VERTICES
    public static class EdgeMap extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private TitanGraph graph;
        private boolean trackState;
        private ModifiableHadoopConfiguration faunusConf;
        private LoaderScriptWrapper loaderScript;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            faunusConf = ModifiableHadoopConfiguration.of(DEFAULT_COMPAT.getContextConfiguration(context));
            graph = TitanGraphOutputMapReduce.generateGraph(faunusConf);
            trackState = context.getConfiguration().getBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_STATE, false);

            // Check whether a script is defined in the config
            if (faunusConf.has(OUTPUT_LOADER_SCRIPT_FILE)) {
                Path scriptPath = new Path(faunusConf.get(OUTPUT_LOADER_SCRIPT_FILE));
                FileSystem scriptFS = FileSystem.get(DEFAULT_COMPAT.getJobContextConfiguration(context));
                loaderScript = new LoaderScriptWrapper(scriptFS, scriptPath);
            }
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            try {
                for (final TitanEdge edge : value.query().queryAll().direction(IN).titanEdges()) {
                    this.getCreateOrDeleteEdge(value, (StandardFaunusEdge)edge, context);
                }
            } catch (final Exception e) {
                graph.rollback();
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.FAILED_TRANSACTIONS, 1L);
                throw new IOException(e.getMessage(), e);
            }
        }

        @Override
        public void cleanup(final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            try {
                graph.commit();
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.SUCCESSFUL_TRANSACTIONS, 1L);
            } catch (Exception e) {
                LOGGER.error("Could not commit transaction during Reduce.cleanup(): ", e);
                graph.rollback();
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.FAILED_TRANSACTIONS, 1L);
                throw new IOException(e.getMessage(), e);
            }
            graph.shutdown();
        }

        public TitanEdge getCreateOrDeleteEdge(final FaunusVertex faunusVertex, final StandardFaunusEdge faunusEdge, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws InterruptedException {

            final Direction dir = IN;
            final TitanVertex titanVertex = this.graph.v(faunusVertex.value(TITAN_ID));

            if (null != loaderScript && loaderScript.hasEdgeMethod()) {
                TitanEdge te = loaderScript.getEdge(faunusEdge, titanVertex, getOtherTitanVertex(faunusVertex, faunusEdge, dir.opposite(), graph), graph, context);
                synchronizeRelationProperties(graph, faunusEdge, te, context);
                return te;
            } else {
                return (TitanEdge) getCreateOrDeleteRelation(graph, trackState, dir, faunusVertex, titanVertex, faunusEdge, context);
            }
        }
    }
}
