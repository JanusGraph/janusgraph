package com.thinkaurelius.titan.hadoop.formats.util.input;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.database.RelationReader;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.types.TypeInspector;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TitanHadoopSetup {

    public TypeInspector getTypeInspector();

    public SystemTypeInspector getSystemTypeInspector();

    public RelationReader getRelationReader(long vertexid);

    public IDManager getIDManager();

    /**
     * Return an input slice across the entire row.
     *
     * TODO This would ideally slice only columns inside the row needed by the query.
     * The slice must include the hidden vertex state property (to filter removed vertices).
     *
     */
    public SliceQuery inputSlice();

    public void close();

    public boolean getFilterPartitionedVertices();

}
