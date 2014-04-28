package com.thinkaurelius.faunus.formats.titan.input;

import com.thinkaurelius.faunus.formats.VertexQueryFilter;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.database.RelationReader;
import com.thinkaurelius.titan.graphdb.types.TypeInspector;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TitanFaunusSetup {

    public TypeInspector getTypeInspector();

    public SystemTypeInspector getSystemTypeInspector();

    public RelationReader getRelationReader();

    public VertexReader getVertexReader();

    public SliceQuery inputSlice(final VertexQueryFilter inputFilter);

    public void close();

}
