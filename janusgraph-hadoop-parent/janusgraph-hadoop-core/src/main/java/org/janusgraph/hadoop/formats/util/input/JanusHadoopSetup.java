package org.janusgraph.hadoop.formats.util.input;

import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.graphdb.database.RelationReader;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.types.TypeInspector;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface JanusHadoopSetup {

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
