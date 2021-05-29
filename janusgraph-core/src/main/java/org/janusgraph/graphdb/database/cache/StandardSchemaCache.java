// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.database.cache;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.relations.EdgeDirection;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.types.system.BaseLabel;
import org.janusgraph.graphdb.types.system.BaseRelationType;
import org.janusgraph.graphdb.types.system.SystemRelationType;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardSchemaCache implements SchemaCache {

    public static final int MAX_CACHED_TYPES_DEFAULT = 10000;

    private static final int INITIAL_CAPACITY = 128;
    private static final int INITIAL_CACHE_SIZE = 16;
    private static final int CACHE_RELATION_MULTIPLIER = 3; // 1) type-name, 2) type-definitions, 3) modifying edges [index, lock]
    private static final int CONCURRENCY_LEVEL = 2;

//    private static final int SCHEMAID_FORW_SHIFT = 4; //Number of bits at the end to append the id of the system type
    private static final int SCHEMAID_TOTALFORW_SHIFT = 3; //Total number of bits appended - the 1 is for the 1 bit direction
    private static final int SCHEMAID_BACK_SHIFT = 2; //Number of bits to remove from end of schema id since its just the padding
    static {
        assert IDManager.VertexIDType.Schema.removePadding(1L<<SCHEMAID_BACK_SHIFT)==1;
        assert SCHEMAID_TOTALFORW_SHIFT-SCHEMAID_BACK_SHIFT>=0;
    }

    private final int maxCachedTypes;
    private final int maxCachedRelations;
    private final StoreRetrieval retriever;

    private volatile ConcurrentMap<String,Long> typeNames;
    private final Cache<String,Long> typeNamesBackup;

    private volatile ConcurrentMap<Long,EntryList> schemaRelations;
    private final Cache<Long,EntryList> schemaRelationsBackup;

    public StandardSchemaCache(final StoreRetrieval retriever) {
        this(MAX_CACHED_TYPES_DEFAULT,retriever);
    }

    public StandardSchemaCache(final int size, final StoreRetrieval retriever) {
        Preconditions.checkArgument(size>0,"Size must be positive");
        Preconditions.checkNotNull(retriever);
        maxCachedTypes = size;
        maxCachedRelations = maxCachedTypes *CACHE_RELATION_MULTIPLIER;
        this.retriever=retriever;

        typeNamesBackup = CacheBuilder.newBuilder()
                .concurrencyLevel(CONCURRENCY_LEVEL).initialCapacity(INITIAL_CACHE_SIZE)
                .maximumSize(maxCachedTypes).build();
        typeNames = new ConcurrentHashMap<>(INITIAL_CAPACITY, 0.75f, CONCURRENCY_LEVEL);

        schemaRelationsBackup = CacheBuilder.newBuilder()
                .concurrencyLevel(CONCURRENCY_LEVEL).initialCapacity(INITIAL_CACHE_SIZE *CACHE_RELATION_MULTIPLIER)
                .maximumSize(maxCachedRelations).build();
//        typeRelations = new ConcurrentHashMap<Long, EntryList>(INITIAL_CAPACITY*CACHE_RELATION_MULTIPLIER,0.75f,CONCURRENCY_LEVEL);
        schemaRelations = new NonBlockingHashMapLong<>(INITIAL_CAPACITY * CACHE_RELATION_MULTIPLIER); //TODO: Is this data structure safe or should we go with ConcurrentHashMap (line above)?
    }


    @Override
    public Long getSchemaId(final String schemaName) {
        ConcurrentMap<String,Long> types = typeNames;
        Long id;
        if (types==null) {
            id = typeNamesBackup.getIfPresent(schemaName);
            if (id==null) {
                id = retriever.retrieveSchemaByName(schemaName);
                if (id!=null) { //only cache if type exists
                    typeNamesBackup.put(schemaName,id);
                }
            }
        } else {
            id = types.get(schemaName);
            if (id==null) { //Retrieve it
                if (types.size()> maxCachedTypes) {
                    /* Safe guard against the concurrent hash map growing to large - this would be a VERY rare event
                    as it only happens for graph databases with thousands of types.
                     */
                    typeNames = null;
                    return getSchemaId(schemaName);
                } else {
                    //Expand map
                    id = retriever.retrieveSchemaByName(schemaName);
                    if (id!=null) { //only cache if type exists
                        types.put(schemaName,id);
                    }
                }
            }
        }
        return id;
    }

    private long getIdentifier(final long schemaId, final SystemRelationType type, final Direction dir) {
        int edgeDir = EdgeDirection.position(dir);
        assert edgeDir==0 || edgeDir==1;

        long typeId = (schemaId >>> SCHEMAID_BACK_SHIFT);
        int systemTypeId;
        if (type== BaseLabel.SchemaDefinitionEdge) systemTypeId=0;
        else if (type== BaseKey.SchemaName) systemTypeId=1;
        else if (type== BaseKey.SchemaCategory) systemTypeId=2;
        else if (type== BaseKey.SchemaDefinitionProperty) systemTypeId=3;
        else throw new AssertionError("Unexpected SystemType encountered in StandardSchemaCache: " + type.name());

        //Ensure that there is enough padding
        assert (systemTypeId<(1<<2));
        return (((typeId<<2)+systemTypeId)<<1)+edgeDir;
    }

    @Override
    public EntryList getSchemaRelations(final long schemaId, final BaseRelationType type, final Direction dir) {
        assert IDManager.isSystemRelationTypeId(type.longId()) && type.longId()>0;
        Preconditions.checkArgument(IDManager.VertexIDType.Schema.is(schemaId));
        Preconditions.checkArgument((Long.MAX_VALUE>>>(SCHEMAID_TOTALFORW_SHIFT-SCHEMAID_BACK_SHIFT))>= schemaId);

        int edgeDir = EdgeDirection.position(dir);
        assert edgeDir==0 || edgeDir==1;

        final long typePlusRelation = getIdentifier(schemaId,type,dir);
        ConcurrentMap<Long,EntryList> types = schemaRelations;
        EntryList entries;
        if (types==null) {
            entries = schemaRelationsBackup.getIfPresent(typePlusRelation);
            if (entries==null) {
                entries = retriever.retrieveSchemaRelations(schemaId, type, dir);
                if (!entries.isEmpty()) { //only cache if type exists
                    schemaRelationsBackup.put(typePlusRelation, entries);
                }
            }
        } else {
            entries = types.get(typePlusRelation);
            if (entries==null) { //Retrieve it
                if (types.size()> maxCachedRelations) {
                    /* Safe guard against the concurrent hash map growing to large - this would be a VERY rare event
                    as it only happens for graph databases with thousands of types.
                     */
                    schemaRelations = null;
                    return getSchemaRelations(schemaId, type, dir);
                } else {
                    //Expand map
                    entries = retriever.retrieveSchemaRelations(schemaId, type, dir);
                    types.put(typePlusRelation,entries);
                }
            }
        }
        assert entries!=null;
        return entries;
    }

    @Override
    public void expireSchemaElement(final long schemaId) {
        //1) expire relations
        final long cutTypeId = (schemaId >>> SCHEMAID_BACK_SHIFT);
        ConcurrentMap<Long,EntryList> types = schemaRelations;
        if (types!=null) {
            types.keySet().removeIf(key -> (key >>> SCHEMAID_TOTALFORW_SHIFT) == cutTypeId);
        }
        for (Long key : schemaRelationsBackup.asMap().keySet()) {
            if ((key >>> SCHEMAID_TOTALFORW_SHIFT) == cutTypeId) schemaRelationsBackup.invalidate(key);
        }
        //2) expire names
        ConcurrentMap<String,Long> names = typeNames;
        if (names!=null) {
            names.entrySet().removeIf(next -> next.getValue().equals(schemaId));
        }
        for (Map.Entry<String,Long> entry : typeNamesBackup.asMap().entrySet()) {
            if (entry.getValue().equals(schemaId)) typeNamesBackup.invalidate(entry.getKey());
        }
    }

}
