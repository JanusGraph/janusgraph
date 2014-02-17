package com.thinkaurelius.titan.graphdb.database.cache;

import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.system.SystemType;
import com.tinkerpop.blueprints.Direction;

import java.util.concurrent.ConcurrentMap;

/**
 * This interface defines the methods that a SchemaCache must implement. A SchemaCache is maintained by the Titan graph
 * database in order to make the frequent lookups of schema vertices and their attributes more efficient through a dedicated
 * caching layer. Schema vertices are type vertices and related vertices.
 *
 * The SchemaCache speeds up two types of lookups:
 * <ul>
 *     <li>Retrieving a type by its name (index lookup)</li>
 *     <li>Retrieving the relations of a schema vertex for predefined {@link SystemType}s</li>
 * </ul>
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface SchemaCache {

    public Long getTypeId(String typeName, StandardTitanTx tx);

    public EntryList getTypeRelations(long schemaId, SystemType type, final Direction dir, StandardTitanTx tx);

    public void expireTypeName(final String name);

    public void expireTypeRelations(final long schemaId);

    public interface StoreRetrieval {

        public Long retrieveTypeByName(final String typeName, final StandardTitanTx tx);

        public EntryList retrieveTypeRelations(final long schemaId, final SystemType type, final Direction dir, final StandardTitanTx tx);

    }

}
