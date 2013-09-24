package com.thinkaurelius.faunus.formats.titan;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.VertexQueryFilter;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.internal.RelationType;
import org.apache.hadoop.conf.Configurable;
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

    public static SliceQuery inputSlice(final VertexQueryFilter inputFilter, final TitanGraph graph) {
        if (inputFilter.limit == 0) {
            final StaticBuffer[] endPoints = IDHandler.getBounds(RelationType.PROPERTY);
            return new SliceQuery(endPoints[0], endPoints[1]).setLimit(Integer.MAX_VALUE);
        } else {
            return DEFAULT_SLICE_QUERY;
        }
    }

}
