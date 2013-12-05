package com.thinkaurelius.faunus.formats.titan.input.current;

import com.google.common.base.Preconditions;
import com.thinkaurelius.faunus.formats.VertexQueryFilter;
import com.thinkaurelius.faunus.formats.titan.TitanInputFormat;
import com.thinkaurelius.faunus.formats.titan.input.TitanFaunusSetup;
import com.thinkaurelius.faunus.formats.titan.util.ConfigurationUtil;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.database.RelationReader;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.internal.RelationType;
import com.thinkaurelius.titan.graphdb.types.TypeInspector;
import com.thinkaurelius.titan.graphdb.types.reference.TypeReferenceContainer;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanFaunusSetupImpl implements TitanFaunusSetup {

    private final GraphDatabaseConfiguration graphConfig;

    private static final StaticBuffer DEFAULT_COLUMN = new StaticByteBuffer(new byte[0]);
    private static final SliceQuery DEFAULT_SLICE_QUERY = new SliceQuery(DEFAULT_COLUMN, DEFAULT_COLUMN);

    public TitanFaunusSetupImpl(final Configuration config) {
        BaseConfiguration titan = ConfigurationUtil.extractConfiguration(config, TitanInputFormat.FAUNUS_GRAPH_INPUT_TITAN);
        graphConfig = new GraphDatabaseConfiguration(titan);
    }

    @Override
    public TypeInspector getTypeInspector() {
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
        return types;
    }

    @Override
    public RelationReader getRelationReader() {
        Serializer serializer = graphConfig.getSerializer();
        EdgeSerializer reader = new EdgeSerializer(serializer);
        return reader;
    }

    @Override
    public SliceQuery inputSlice(final VertexQueryFilter inputFilter) {
        if (inputFilter.limit == 0) {
            final StaticBuffer[] endPoints = IDHandler.getBounds(RelationType.PROPERTY);
            return new SliceQuery(endPoints[0], endPoints[1]).setLimit(Integer.MAX_VALUE);
        } else {
            return DEFAULT_SLICE_QUERY;
        }
    }
}
