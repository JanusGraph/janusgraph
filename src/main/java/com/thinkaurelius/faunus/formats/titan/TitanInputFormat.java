package com.thinkaurelius.faunus.formats.titan;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.VertexQueryFilter;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;

import java.nio.ByteBuffer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class TitanInputFormat extends InputFormat<NullWritable, FaunusVertex> implements Configurable {

    public static final String FAUNUS_GRAPH_INPUT_TITAN_STORAGE_HOSTNAME = "faunus.graph.input.titan.storage.hostname";
    public static final String FAUNUS_GRAPH_INPUT_TITAN_STORAGE_PORT = "faunus.graph.input.titan.storage.port";
    public static final String FAUNUS_GRAPH_INPUT_TITAN = "faunus.graph.input.titan";

    private static final ByteBuffer DEFAULT_COLUMN = ByteBuffer.wrap(new byte[0]);

    public static SliceQuery inputSlice(final VertexQueryFilter inputFilter, final TitanGraph graph) {
        if (inputFilter.limit == 0) {
            final IDManager idManager = (IDManager) ((StandardTitanGraph) graph).getIDInspector();
            final ByteBuffer startColumn, endColumn;
            startColumn = IDHandler.getEdgeTypeGroup(0, 0, idManager);
            endColumn = IDHandler.getEdgeTypeGroup(idManager.getMaxGroupID() + 1, 0, idManager);
            return new SliceQuery(startColumn, endColumn, Integer.MAX_VALUE);
        } else {
            return new SliceQuery(DEFAULT_COLUMN, DEFAULT_COLUMN, Integer.MAX_VALUE);
        }
    }

}
