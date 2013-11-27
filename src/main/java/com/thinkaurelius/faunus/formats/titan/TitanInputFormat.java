package com.thinkaurelius.faunus.formats.titan;

import com.google.common.base.Preconditions;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.VertexQueryFilter;
import com.thinkaurelius.faunus.formats.titan.util.ConfigurationUtil;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.internal.RelationType;
import com.thinkaurelius.titan.graphdb.types.reference.TypeReferenceContainer;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class TitanInputFormat extends InputFormat<NullWritable, FaunusVertex> implements Configurable {

    public static final String FAUNUS_GRAPH_INPUT_TITAN_STORAGE_HOSTNAME = "faunus.graph.input.titan.storage.hostname";
    public static final String FAUNUS_GRAPH_INPUT_TITAN_STORAGE_PORT = "faunus.graph.input.titan.storage.port";
    public static final String FAUNUS_GRAPH_INPUT_TITAN = "faunus.graph.input.titan";

    private static final StaticBuffer DEFAULT_COLUMN = new StaticByteBuffer(new byte[0]);
    private static final SliceQuery DEFAULT_SLICE_QUERY = new SliceQuery(DEFAULT_COLUMN, DEFAULT_COLUMN);

    protected VertexQueryFilter vertexQuery;
    protected boolean pathEnabled;

    public static SliceQuery inputSlice(final VertexQueryFilter inputFilter) {
        if (inputFilter.limit == 0) {
            final StaticBuffer[] endPoints = IDHandler.getBounds(RelationType.PROPERTY);
            return new SliceQuery(endPoints[0], endPoints[1]).setLimit(Integer.MAX_VALUE);
        } else {
            return DEFAULT_SLICE_QUERY;
        }
    }

    @Override
    public void setConf(final Configuration config) {
        this.vertexQuery = VertexQueryFilter.create(config);
        this.pathEnabled = config.getBoolean(FaunusCompiler.PATH_ENABLED, false);
    }

    public Setup setupConfiguration(final Configuration config) {
        BaseConfiguration titan = ConfigurationUtil.extractConfiguration(config, FAUNUS_GRAPH_INPUT_TITAN);
        GraphDatabaseConfiguration graphConfig = new GraphDatabaseConfiguration(titan);
        StandardTitanGraph graph = null;
        TypeReferenceContainer types = null;
        try {
            graph = new StandardTitanGraph(graphConfig);
            types = new TypeReferenceContainer(graph);
        } catch (Exception e) {
            throw new RuntimeException("Could not read schema from TitanGraph",e);
        } finally {
            if (graph!=null) graph.shutdown();
        }
        Preconditions.checkNotNull(types);
        Serializer serializer = graphConfig.getSerializer();
        return new Setup(serializer,types);
    }

    protected static final class Setup {

        public final Serializer serializer;
        public final TypeReferenceContainer types;

        public Setup(Serializer serializer, TypeReferenceContainer types) {
            this.serializer = serializer;
            this.types = types;
        }
    }

}
