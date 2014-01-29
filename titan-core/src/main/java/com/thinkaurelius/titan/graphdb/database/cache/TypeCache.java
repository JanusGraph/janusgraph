package com.thinkaurelius.titan.graphdb.database.cache;

import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.system.SystemType;
import com.tinkerpop.blueprints.Direction;

import java.util.concurrent.ConcurrentMap;

/**
 * This interface defines the methods that a TypeCache must implement. A TypeCache is maintained by the Titan graph
 * database in order to make the frequent lookups of types and their attributes more efficient through a dedicated
 * caching layer.
 *
 * The TypeCache speeds up two types of lookups:
 * <ul>
 *     <li>Retrieving a type by its name (index lookup)</li>
 *     <li>Retrieving the relations of a type for predefined {@link SystemType}s</li>
 * </ul>
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TypeCache {

    public Long getTypeId(String typename, StandardTitanTx tx);

    public EntryList getTypeRelations(long typeid, SystemType type, final Direction dir, StandardTitanTx tx);

    public void expireTypeName(final String name);

    public void expireTypeRelations(final long typeid);

    public interface StoreRetrieval {

        public Long retrieveTypeByName(final String typename, final StandardTitanTx tx);

        public EntryList retrieveTypeRelations(final long typeid, final SystemType type, final Direction dir, final StandardTitanTx tx);

    }

}
