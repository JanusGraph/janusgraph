package com.thinkaurelius.titan.graphdb.database.cache;

import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.graphdb.types.system.BaseRelationType;
import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 * This interface defines the methods that a SchemaCache must implement. A SchemaCache is maintained by the Titan graph
 * database in order to make the frequent lookups of schema vertices and their attributes more efficient through a dedicated
 * caching layer. Schema vertices are type vertices and related vertices.
 *
 * The SchemaCache speeds up two types of lookups:
 * <ul>
 *     <li>Retrieving a type by its name (index lookup)</li>
 *     <li>Retrieving the relations of a schema vertex for predefined {@link com.thinkaurelius.titan.graphdb.types.system.SystemRelationType}s</li>
 * </ul>
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface SchemaCache {

    public Long getSchemaId(String schemaName);

    public EntryList getSchemaRelations(long schemaId, BaseRelationType type, final Direction dir);

    public void expireSchemaElement(final long schemaId);

    public interface StoreRetrieval {

        public Long retrieveSchemaByName(final String typeName);

        public EntryList retrieveSchemaRelations(final long schemaId, final BaseRelationType type, final Direction dir);

    }

}
