package com.thinkaurelius.faunus.formats.titan.input;

import com.google.common.base.Preconditions;
import com.thinkaurelius.faunus.formats.VertexQueryFilter;
import com.thinkaurelius.faunus.formats.titan.util.ConfigurationUtil;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.database.RelationReader;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.types.TypeInspector;
import com.thinkaurelius.titan.graphdb.types.reference.TypeReferenceContainer;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TitanFaunusSetup {

    public TypeInspector getTypeInspector();

    public RelationReader getRelationReader();

    public SliceQuery inputSlice(final VertexQueryFilter inputFilter);

    public void close();

}
