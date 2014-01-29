package com.thinkaurelius.titan.graphdb.database.cache;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.system.SystemType;
import com.thinkaurelius.titan.graphdb.types.system.SystemTypeManager;
import com.tinkerpop.blueprints.Direction;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardTypeCache implements TypeCache {

    public static final int MAX_CACHED_TYPES_DEFAULT = 10000;

    private static final int INITIAL_CAPACITY = 128;
    private static final int INITIAL_CACHE_SIZE = 16;
    private static final int CACHE_RELATION_MULTIPLIER = 3; // 1) type-name, 2) type-definitions, 3) modifying edges [index, lock]
    private static final int CONCURRENCY_LEVEL = 2;

    private static final int TYPE_ID_BITSHIFT = 4;
    private static final int TYPE_ID_TOTALSHIFT = TYPE_ID_BITSHIFT+1;
    private static final int TYPE_ID_BACKSHIFT = 3;

    private final int maxCachedTypes;
    private final int maxCachedRelations;
    private final StoreRetrieval retriever;

    private volatile ConcurrentMap<String,Long> typeNames;
    private final Cache<String,Long> typeNamesBackup;

    private volatile ConcurrentMap<Long,EntryList> typeRelations;
    private final Cache<Long,EntryList> typeRelationsBackup;

    public StandardTypeCache(final StoreRetrieval retriever) {
        this(MAX_CACHED_TYPES_DEFAULT,retriever);
    }

    public StandardTypeCache(final int size, final StoreRetrieval retriever) {
        Preconditions.checkArgument(size>0,"Size must be positive");
        Preconditions.checkNotNull(retriever);
        maxCachedTypes = size;
        maxCachedRelations = maxCachedTypes *CACHE_RELATION_MULTIPLIER;
        this.retriever=retriever;

        typeNamesBackup = CacheBuilder.<String,Long>newBuilder()
                .concurrencyLevel(CONCURRENCY_LEVEL).initialCapacity(INITIAL_CACHE_SIZE)
                .maximumSize(maxCachedTypes).build();
        typeNames = new ConcurrentHashMap<String, Long>(INITIAL_CAPACITY,0.75f,CONCURRENCY_LEVEL);

        typeRelationsBackup = CacheBuilder.<Long,EntryList>newBuilder()
                .concurrencyLevel(CONCURRENCY_LEVEL).initialCapacity(INITIAL_CACHE_SIZE *CACHE_RELATION_MULTIPLIER)
                .maximumSize(maxCachedRelations).build();
//        typeRelations = new ConcurrentHashMap<Long, EntryList>(INITIAL_CAPACITY*CACHE_RELATION_MULTIPLIER,0.75f,CONCURRENCY_LEVEL);
        typeRelations = new NonBlockingHashMapLong<EntryList>(INITIAL_CAPACITY*CACHE_RELATION_MULTIPLIER); //TODO: Is this data structure safe or should we go with ConcurrentHashMap (line above)?
    }


    @Override
    public Long getTypeId(final String typename, final StandardTitanTx tx) {
        ConcurrentMap<String,Long> types = typeNames;
        Long id;
        if (types==null) {
            id = typeNamesBackup.getIfPresent(typename);
            if (id==null) {
                id = retriever.retrieveTypeByName(typename, tx);
                if (id!=null) { //only cache if type exists
                    typeNamesBackup.put(typename,id);
                }
            }
        } else {
            id = types.get(typename);
            if (id==null) { //Retrieve it
                if (types.size()> maxCachedTypes) {
                    /* Safe guard against the concurrent hash map growing to large - this would be a VERY rare event
                    as it only happens for graph databases with thousands of types.
                     */
                    typeNames = null;
                    return getTypeId(typename, tx);
                } else {
                    //Expand map
                    id = retriever.retrieveTypeByName(typename, tx);
                    if (id!=null) { //only cache if type exists
                        types.put(typename,id);
                    }
                }
            }
        }
        return id;
    }

    @Override
    public EntryList getTypeRelations(final long typeid, final SystemType type, final Direction dir, final StandardTitanTx tx) {
        assert IDManager.isEdgeLabelID(typeid) || IDManager.isPropertyKeyID(typeid);
        assert IDManager.IDType.EdgeLabel.offset()==TYPE_ID_BACKSHIFT && IDManager.IDType.PropertyKey.offset()==TYPE_ID_BACKSHIFT;
        assert SystemTypeManager.SYSTEM_TYPE_OFFSET <= (1<<TYPE_ID_BITSHIFT);
        assert IDManager.getTypeCount(type.getID())<SystemTypeManager.SYSTEM_TYPE_OFFSET;
        Preconditions.checkArgument((Long.MAX_VALUE>>>(TYPE_ID_BITSHIFT-TYPE_ID_BACKSHIFT))>=typeid);
        int edgeDir = EdgeDirection.position(dir);
        assert edgeDir==0 || edgeDir==1;

        final long typePlusRelation = ((((typeid >>> TYPE_ID_BACKSHIFT) << 1) + edgeDir) << TYPE_ID_BITSHIFT) + IDManager.getTypeCount(type.getID());
        ConcurrentMap<Long,EntryList> types = typeRelations;
        EntryList entries;
        if (types==null) {
            entries = typeRelationsBackup.getIfPresent(typePlusRelation);
            if (entries==null) {
                entries = retriever.retrieveTypeRelations(typeid,type,dir,tx);
                if (!entries.isEmpty()) { //only cache if type exists
                    typeRelationsBackup.put(typePlusRelation,entries);
                }
            }
        } else {
            entries = types.get(typePlusRelation);
            if (entries==null) { //Retrieve it
                if (types.size()> maxCachedRelations) {
                    /* Safe guard against the concurrent hash map growing to large - this would be a VERY rare event
                    as it only happens for graph databases with thousands of types.
                     */
                    typeRelations = null;
                    return getTypeRelations(typeid, type, dir, tx);
                } else {
                    //Expand map
                    entries = retriever.retrieveTypeRelations(typeid, type, dir, tx);
                    if (!entries.isEmpty()) { //only cache if type exists
                        types.put(typePlusRelation,entries);
                    }
                }
            }
        }
        assert entries!=null;
        return entries;
    }

    @Override
    public void expireTypeName(final String name) {
        ConcurrentMap<String,Long> types = typeNames;
        if (types!=null) types.remove(name);
        typeNamesBackup.invalidate(name);
    }

    @Override
    public void expireTypeRelations(final long typeid) {
        final long cuttypeid = (typeid >>> TYPE_ID_BACKSHIFT);
        ConcurrentMap<Long,EntryList> types = typeRelations;
        if (types!=null) {
            Iterator<Long> keys = types.keySet().iterator();
            while (keys.hasNext()) {
                long key = keys.next();
                if ((key>>>TYPE_ID_TOTALSHIFT)==cuttypeid) keys.remove();
            }
        }
        Iterator<Long> keys = typeRelationsBackup.asMap().keySet().iterator();
        while (keys.hasNext()) {
            long key = keys.next();
            if ((key>>>TYPE_ID_TOTALSHIFT)==cuttypeid) typeRelationsBackup.invalidate(key);
        }
    }

}
