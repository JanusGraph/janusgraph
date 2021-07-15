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

package org.janusgraph.graphdb.database;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.SchemaViolationException;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.MetaAnnotatable;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexFeatures;
import org.janusgraph.diskstorage.indexing.IndexInformation;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.diskstorage.indexing.StandardKeyInformation;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.HashingUtil;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.InternalAttributeUtil;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.ConditionUtil;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.query.graph.IndexQueryBuilder;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.janusgraph.graphdb.query.graph.MultiKeySliceQuery;
import org.janusgraph.graphdb.query.index.IndexSelectionUtil;
import org.janusgraph.graphdb.query.vertex.VertexCentricQueryBuilder;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.IndexField;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.ParameterIndexField;
import org.janusgraph.graphdb.types.ParameterType;
import org.janusgraph.util.encoding.LongEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_NAME_MAPPING;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IndexSerializer {

    private static final Logger log = LoggerFactory.getLogger(IndexSerializer.class);

    private static final int DEFAULT_OBJECT_BYTELEN = 30;
    private static final byte FIRST_INDEX_COLUMN_BYTE = 0;

    private final Serializer serializer;
    private final Configuration configuration;
    private final Map<String, ? extends IndexInformation> mixedIndexes;

    private final boolean hashKeys;
    private final HashingUtil.HashLength hashLength = HashingUtil.HashLength.SHORT;

    public IndexSerializer(Configuration config, Serializer serializer, Map<String, ? extends IndexInformation> indexes, final boolean hashKeys) {
        this.serializer = serializer;
        this.configuration = config;
        this.mixedIndexes = indexes;
        this.hashKeys=hashKeys;
        if (hashKeys) log.info("Hashing index keys");
    }


    /* ################################################
               Index Information
    ################################################### */

    public boolean containsIndex(final String indexName) {
        return mixedIndexes.containsKey(indexName);
    }

    public String getDefaultFieldName(final PropertyKey key, final Parameter[] parameters, final String indexName) {
        Preconditions.checkArgument(!ParameterType.MAPPED_NAME.hasParameter(parameters), "A field name mapping has been specified for key: %s",key);
        Preconditions.checkArgument(containsIndex(indexName), "Unknown backing index: %s", indexName);
        final String fieldname = configuration.get(INDEX_NAME_MAPPING,indexName)?key.name():keyID2Name(key);
        return mixedIndexes.get(indexName).mapKey2Field(fieldname,
            new StandardKeyInformation(key,parameters));
    }

    public static void register(final MixedIndexType index, final PropertyKey key, final BackendTransaction tx) throws BackendException {
        tx.getIndexTransaction(index.getBackingIndexName()).register(index.getStoreName(), key2Field(index,key), getKeyInformation(index.getField(key)));
    }

    public boolean supports(final MixedIndexType index, final ParameterIndexField field) {
        return getMixedIndex(index).supports(getKeyInformation(field));
    }

    public boolean supports(final MixedIndexType index, final ParameterIndexField field, final JanusGraphPredicate predicate) {
        return getMixedIndex(index).supports(getKeyInformation(field),predicate);
    }

    public boolean supportsExistsQuery(final MixedIndexType index, final ParameterIndexField field) {
        if (Geoshape.class.equals(getKeyInformation(field).getDataType())) {
            return getMixedIndex(index).getFeatures().supportsGeoExists();
        }
        return true;
    }

    public IndexFeatures features(final MixedIndexType index) {
        return getMixedIndex(index).getFeatures();
    }

    private IndexInformation getMixedIndex(final MixedIndexType index) {
        final IndexInformation indexinfo = mixedIndexes.get(index.getBackingIndexName());
        Preconditions.checkArgument(indexinfo != null, "Index is unknown or not configured: %s", index.getBackingIndexName());
        return indexinfo;
    }

    private static StandardKeyInformation getKeyInformation(final ParameterIndexField field) {
        return new StandardKeyInformation(field.getFieldKey(),field.getParameters());
    }

    public IndexInfoRetriever getIndexInfoRetriever(StandardJanusGraphTx tx) {
        return new IndexInfoRetriever(tx);
    }

    public static class IndexInfoRetriever implements KeyInformation.Retriever {

        private final StandardJanusGraphTx transaction;

        private IndexInfoRetriever(StandardJanusGraphTx tx) {
            Preconditions.checkNotNull(tx);
            transaction=tx;
        }

        @Override
        public KeyInformation.IndexRetriever get(final String index) {
            return new KeyInformation.IndexRetriever() {

                final Map<String,KeyInformation.StoreRetriever> indexes = new ConcurrentHashMap<>();

                @Override
                public KeyInformation get(String store, String key) {
                    return get(store).get(key);
                }

                @Override
                public KeyInformation.StoreRetriever get(final String store) {
                    if (indexes.get(store)==null) {
                        Preconditions.checkNotNull(transaction,"Retriever has not been initialized");
                        final MixedIndexType extIndex = getMixedIndex(store, transaction);
                        assert extIndex.getBackingIndexName().equals(index);
                        final ImmutableMap.Builder<String,KeyInformation> b = ImmutableMap.builder();
                        for (final ParameterIndexField field : extIndex.getFieldKeys()) b.put(key2Field(field),getKeyInformation(field));
                        ImmutableMap<String,KeyInformation> infoMap;
                        try {
                            infoMap = b.build();
                        } catch (IllegalArgumentException e) {
                            throw new JanusGraphException("Duplicate index field names found, likely you have multiple properties mapped to the same index field", e);
                        }
                        final KeyInformation.StoreRetriever storeRetriever = infoMap::get;
                        indexes.put(store,storeRetriever);
                    }
                    return indexes.get(store);
                }

                @Override
                public void invalidate(final String store) {
                    indexes.remove(store);
                }
            };
        }
    }

    /* ################################################
               Index Updates
    ################################################### */

    public static class IndexUpdate<K,E> {

        private enum Type { ADD, DELETE }

        private final IndexType index;
        private final Type mutationType;
        private final K key;
        private final E entry;
        private final JanusGraphElement element;

        private IndexUpdate(IndexType index, Type mutationType, K key, E entry, JanusGraphElement element) {
            assert index!=null && mutationType!=null && key!=null && entry!=null && element!=null;
            assert !index.isCompositeIndex() || (key instanceof StaticBuffer && entry instanceof Entry);
            assert !index.isMixedIndex() || (key instanceof String && entry instanceof IndexEntry);
            this.index = index;
            this.mutationType = mutationType;
            this.key = key;
            this.entry = entry;
            this.element = element;
        }

        public JanusGraphElement getElement() {
            return element;
        }

        public IndexType getIndex() {
            return index;
        }

        public Type getType() {
            return mutationType;
        }

        public K getKey() {
            return key;
        }

        public E getEntry() {
            return entry;
        }

        public boolean isAddition() {
            return mutationType==Type.ADD;
        }

        public boolean isDeletion() {
            return mutationType==Type.DELETE;
        }

        public boolean isCompositeIndex() {
            return index.isCompositeIndex();
        }

        public boolean isMixedIndex() {
            return index.isMixedIndex();
        }

        public void setTTL(int ttl) {
            Preconditions.checkArgument(ttl>0 && mutationType==Type.ADD);
            ((MetaAnnotatable)entry).setMetaData(EntryMetaData.TTL,ttl);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, mutationType, key, entry);
        }

        @Override
        public boolean equals(Object other) {
            if (this==other) return true;
            else if (other==null || !(other instanceof IndexUpdate)) return false;
            final IndexUpdate oth = (IndexUpdate)other;
            return index.equals(oth.index) && mutationType==oth.mutationType && key.equals(oth.key) && entry.equals(oth.entry);
        }
    }

    private static IndexUpdate.Type getUpdateType(InternalRelation relation) {
        assert relation.isNew() || relation.isRemoved();
        return (relation.isNew()? IndexUpdate.Type.ADD : IndexUpdate.Type.DELETE);
    }

    private static boolean indexAppliesTo(IndexType index, JanusGraphElement element) {
        return index.getElement().isInstance(element) &&
            (!(index instanceof CompositeIndexType) || ((CompositeIndexType)index).getStatus()!=SchemaStatus.DISABLED) &&
            (!index.hasSchemaTypeConstraint() ||
                index.getElement().matchesConstraint(index.getSchemaTypeConstraint(),element));
    }

    public Collection<IndexUpdate> getIndexUpdates(InternalRelation relation) {
        assert relation.isNew() || relation.isRemoved();
        final Set<IndexUpdate> updates = Sets.newHashSet();
        final IndexUpdate.Type updateType = getUpdateType(relation);
        final int ttl = updateType==IndexUpdate.Type.ADD?StandardJanusGraph.getTTL(relation):0;
        for (final PropertyKey type : relation.getPropertyKeysDirect()) {
            if (type == null) continue;
            for (final IndexType index : ((InternalRelationType) type).getKeyIndexes()) {
                if (!indexAppliesTo(index,relation)) continue;
                IndexUpdate update;
                if (index instanceof CompositeIndexType) {
                    final CompositeIndexType iIndex= (CompositeIndexType) index;
                    final RecordEntry[] record = indexMatch(relation, iIndex);
                    if (record==null) continue;
                    update = new IndexUpdate<>(iIndex, updateType, getIndexKey(iIndex, record), getIndexEntry(iIndex, record, relation), relation);
                } else {
                    assert relation.valueOrNull(type)!=null;
                    if (((MixedIndexType)index).getField(type).getStatus()== SchemaStatus.DISABLED) continue;
                    update = getMixedIndexUpdate(relation, type, relation.valueOrNull(type), (MixedIndexType) index, updateType);
                }
                if (ttl>0) update.setTTL(ttl);
                updates.add(update);
            }
        }
        return updates;
    }

    private static PropertyKey[] getKeysOfRecords(RecordEntry[] record) {
        final PropertyKey[] keys = new PropertyKey[record.length];
        for (int i=0;i<record.length;i++) keys[i]=record[i].key;
        return keys;
    }

    private static int getIndexTTL(InternalVertex vertex, PropertyKey... keys) {
        int ttl = StandardJanusGraph.getTTL(vertex);
        for (final PropertyKey key : keys) {
            final int kttl = ((InternalRelationType) key).getTTL();
            if (kttl > 0 && (kttl < ttl || ttl <= 0)) ttl = kttl;
        }
        return ttl;
    }

    public Collection<IndexUpdate> getIndexUpdates(InternalVertex vertex, Collection<InternalRelation> updatedProperties) {
        if (updatedProperties.isEmpty()) return Collections.emptyList();
        final Set<IndexUpdate> updates = Sets.newHashSet();

        for (final InternalRelation rel : updatedProperties) {
            assert rel.isProperty();
            final JanusGraphVertexProperty p = (JanusGraphVertexProperty)rel;
            assert rel.isNew() || rel.isRemoved(); assert rel.getVertex(0).equals(vertex);
            final IndexUpdate.Type updateType = getUpdateType(rel);
            for (final IndexType index : ((InternalRelationType)p.propertyKey()).getKeyIndexes()) {
                if (!indexAppliesTo(index,vertex)) continue;
                if (index.isCompositeIndex()) { //Gather composite indexes
                    final CompositeIndexType cIndex = (CompositeIndexType)index;
                    final IndexRecords updateRecords = indexMatches(vertex,cIndex,updateType==IndexUpdate.Type.DELETE,p.propertyKey(),new RecordEntry(p));
                    for (final RecordEntry[] record : updateRecords) {
                        final IndexUpdate update = new IndexUpdate<>(cIndex, updateType, getIndexKey(cIndex, record), getIndexEntry(cIndex, record, vertex), vertex);
                        final int ttl = getIndexTTL(vertex,getKeysOfRecords(record));
                        if (ttl>0 && updateType== IndexUpdate.Type.ADD) update.setTTL(ttl);
                        updates.add(update);
                    }
                } else { //Update mixed indexes
                    ParameterIndexField field = ((MixedIndexType)index).getField(p.propertyKey());
                    if (field == null) {
                        throw new SchemaViolationException(p.propertyKey() + " is not available in mixed index " + index);
                    }
                    if (field.getStatus() == SchemaStatus.DISABLED) continue;
                    final IndexUpdate update = getMixedIndexUpdate(vertex, p.propertyKey(), p.value(), (MixedIndexType) index, updateType);
                    final int ttl = getIndexTTL(vertex,p.propertyKey());
                    if (ttl>0 && updateType== IndexUpdate.Type.ADD) update.setTTL(ttl);
                    updates.add(update);
                }
            }
        }
        return updates;
    }

    private IndexUpdate<String,IndexEntry> getMixedIndexUpdate(JanusGraphElement element, PropertyKey key, Object value,
                                                               MixedIndexType index, IndexUpdate.Type updateType)  {
        return new IndexUpdate<>(index, updateType, element2String(element), new IndexEntry(key2Field(index.getField(key)), value), element);
    }

    public boolean reindexElement(JanusGraphElement element, MixedIndexType index, Map<String,Map<String,List<IndexEntry>>> documentsPerStore) {
        if (!indexAppliesTo(index, element))
            return false;
        final List<IndexEntry> entries = Lists.newArrayList();
        for (final ParameterIndexField field: index.getFieldKeys()) {
            final PropertyKey key = field.getFieldKey();
            if (field.getStatus()==SchemaStatus.DISABLED) continue;
            if (element.properties(key.name()).hasNext()) {
                element.values(key.name()).forEachRemaining(value->entries.add(new IndexEntry(key2Field(field), value)));
            }
        }
        if (entries.isEmpty())
            return false;
        getDocuments(documentsPerStore, index).put(element2String(element), entries);
        return true;
    }

    private Map<String,List<IndexEntry>> getDocuments(Map<String,Map<String,List<IndexEntry>>> documentsPerStore, MixedIndexType index) {
        return documentsPerStore.computeIfAbsent(index.getStoreName(), k -> Maps.newHashMap());
    }

    public void removeElement(Object elementId, MixedIndexType index, Map<String,Map<String,List<IndexEntry>>> documentsPerStore) {
        Preconditions.checkArgument((index.getElement()==ElementCategory.VERTEX && elementId instanceof Long) ||
            (index.getElement().isRelation() && elementId instanceof RelationIdentifier),"Invalid element id [%s] provided for index: %s",elementId,index);
        getDocuments(documentsPerStore,index).put(element2String(elementId),Lists.newArrayList());
    }

    public Set<IndexUpdate<StaticBuffer,Entry>> reindexElement(JanusGraphElement element, CompositeIndexType index) {
        final Set<IndexUpdate<StaticBuffer,Entry>> indexEntries = Sets.newHashSet();
        if (!indexAppliesTo(index,element)) return indexEntries;
        Iterable<RecordEntry[]> records;
        if (element instanceof JanusGraphVertex) records = indexMatches((JanusGraphVertex)element,index);
        else {
            assert element instanceof JanusGraphRelation;
            records = Collections.EMPTY_LIST;
            final RecordEntry[] record = indexMatch((JanusGraphRelation)element,index);
            if (record!=null) records = ImmutableList.of(record);
        }
        for (final RecordEntry[] record : records) {
            indexEntries.add(new IndexUpdate<>(index, IndexUpdate.Type.ADD, getIndexKey(index, record), getIndexEntry(index, record, element), element));
        }
        return indexEntries;
    }

    public static RecordEntry[] indexMatch(JanusGraphRelation relation, CompositeIndexType index) {
        final IndexField[] fields = index.getFieldKeys();
        final RecordEntry[] match = new RecordEntry[fields.length];
        for (int i = 0; i <fields.length; i++) {
            final IndexField f = fields[i];
            final Object value = relation.valueOrNull(f.getFieldKey());
            if (value==null) return null; //No match
            match[i] = new RecordEntry(relation.longId(),value,f.getFieldKey());
        }
        return match;
    }

    public static class IndexRecords extends ArrayList<RecordEntry[]> {

        @Override
        public boolean add(RecordEntry[] record) {
            return super.add(Arrays.copyOf(record,record.length));
        }

        public Iterable<Object[]> getRecordValues() {
            return Iterables.transform(this, new Function<RecordEntry[], Object[]>() {
                @Nullable
                @Override
                public Object[] apply(final RecordEntry[] record) {
                    return getValues(record);
                }
            });
        }

        private static Object[] getValues(RecordEntry[] record) {
            final Object[] values = new Object[record.length];
            for (int i = 0; i < values.length; i++) {
                values[i]=record[i].value;
            }
            return values;
        }

    }

    private static class RecordEntry {

        final long relationId;
        final Object value;
        final PropertyKey key;

        private RecordEntry(long relationId, Object value, PropertyKey key) {
            this.relationId = relationId;
            this.value = value;
            this.key = key;
        }

        private RecordEntry(JanusGraphVertexProperty property) {
            this(property.longId(),property.value(),property.propertyKey());
        }
    }

    public static IndexRecords indexMatches(JanusGraphVertex vertex, CompositeIndexType index) {
        return indexMatches(vertex,index,null,null);
    }

    public static IndexRecords indexMatches(JanusGraphVertex vertex, CompositeIndexType index,
                                            PropertyKey replaceKey, Object replaceValue) {
        final IndexRecords matches = new IndexRecords();
        final IndexField[] fields = index.getFieldKeys();
        if (indexAppliesTo(index,vertex)) {
            indexMatches(vertex,new RecordEntry[fields.length],matches,fields,0,false,
                replaceKey,new RecordEntry(0,replaceValue,replaceKey));
        }
        return matches;
    }

    private static IndexRecords indexMatches(JanusGraphVertex vertex, CompositeIndexType index,
                                             boolean onlyLoaded, PropertyKey replaceKey, RecordEntry replaceValue) {
        final IndexRecords matches = new IndexRecords();
        final IndexField[] fields = index.getFieldKeys();
        indexMatches(vertex,new RecordEntry[fields.length],matches,fields,0,onlyLoaded,replaceKey,replaceValue);
        return matches;
    }

    private static void indexMatches(JanusGraphVertex vertex, RecordEntry[] current, IndexRecords matches,
                                     IndexField[] fields, int pos,
                                     boolean onlyLoaded, PropertyKey replaceKey, RecordEntry replaceValue) {
        if (pos>= fields.length) {
            matches.add(current);
            return;
        }

        final PropertyKey key = fields[pos].getFieldKey();

        List<RecordEntry> values;
        if (key.equals(replaceKey)) {
            values = ImmutableList.of(replaceValue);
        } else {
            values = new ArrayList<>();
            Iterable<JanusGraphVertexProperty> props;
            if (onlyLoaded ||
                (!vertex.isNew() && IDManager.VertexIDType.PartitionedVertex.is(vertex.longId()))) {
                //going through transaction so we can query deleted vertices
                final VertexCentricQueryBuilder qb = ((InternalVertex)vertex).tx().query(vertex);
                qb.noPartitionRestriction().type(key);
                if (onlyLoaded) qb.queryOnlyLoaded();
                props = qb.properties();
            } else {
                props = vertex.query().keys(key.name()).properties();
            }
            for (final JanusGraphVertexProperty p : props) {
                assert !onlyLoaded || p.isLoaded() || p.isRemoved();
                assert key.dataType().equals(p.value().getClass()) : key + " -> " + p;
                values.add(new RecordEntry(p));
            }
        }
        for (final RecordEntry value : values) {
            current[pos]=value;
            indexMatches(vertex,current,matches,fields,pos+1,onlyLoaded,replaceKey,replaceValue);
        }
    }


    /* ################################################
                Querying
    ################################################### */

    public Stream<Object> query(final JointIndexQuery.Subquery query, final BackendTransaction tx) {
        final IndexType index = query.getIndex();
        if (index.isCompositeIndex()) {
            final MultiKeySliceQuery sq = query.getCompositeQuery();
            final List<EntryList> rs = sq.execute(tx);
            final List<Object> results = new ArrayList<>(rs.get(0).size());
            for (final EntryList r : rs) {
                for (final java.util.Iterator<Entry> iterator = r.reuseIterator(); iterator.hasNext(); ) {
                    final Entry entry = iterator.next();
                    final ReadBuffer entryValue = entry.asReadBuffer();
                    entryValue.movePositionTo(entry.getValuePosition());
                    switch(index.getElement()) {
                        case VERTEX:
                            results.add(VariableLong.readPositive(entryValue));
                            break;
                        default:
                            results.add(bytebuffer2RelationId(entryValue));
                    }
                }
            }
            return results.stream();
        } else {
            return tx.indexQuery(index.getBackingIndexName(), query.getMixedQuery()).map(IndexSerializer::string2ElementId);
        }
    }

    public Long queryCount(final JointIndexQuery.Subquery query, final BackendTransaction tx) {
        final IndexType index = query.getIndex();
        assert index.isMixedIndex();
        return tx.indexQueryCount(index.getBackingIndexName(), query.getMixedQuery());
    }

    public MultiKeySliceQuery getQuery(final CompositeIndexType index, List<Object[]> values) {
        final List<KeySliceQuery> ksqs = new ArrayList<>(values.size());
        for (final Object[] value : values) {
            ksqs.add(new KeySliceQuery(getIndexKey(index,value), BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(1)));
        }
        return new MultiKeySliceQuery(ksqs);
    }

    public IndexQuery getQuery(final MixedIndexType index, final Condition condition, final OrderList orders) {
        final Condition newCondition = ConditionUtil.literalTransformation(condition,
            new Function<Condition<JanusGraphElement>, Condition<JanusGraphElement>>() {
                @Nullable
                @Override
                public Condition<JanusGraphElement> apply(final Condition<JanusGraphElement> condition) {
                    Preconditions.checkArgument(condition instanceof PredicateCondition);
                    final PredicateCondition pc = (PredicateCondition) condition;
                    final PropertyKey key = (PropertyKey) pc.getKey();
                    return new PredicateCondition<>(key2Field(index, key), pc.getPredicate(), pc.getValue());
                }
            });
        ImmutableList<IndexQuery.OrderEntry> newOrders = IndexQuery.NO_ORDER;
        if (!orders.isEmpty() && IndexSelectionUtil.indexCoversOrder(index,orders)) {
            final ImmutableList.Builder<IndexQuery.OrderEntry> lb = ImmutableList.builder();
            for (int i = 0; i < orders.size(); i++) {
                lb.add(new IndexQuery.OrderEntry(key2Field(index,orders.getKey(i)), orders.getOrder(i), orders.getKey(i).dataType()));
            }
            newOrders = lb.build();
        }
        return new IndexQuery(index.getStoreName(), newCondition, newOrders);
    }


    /* ################################################
    	Common code used by executeQuery and executeTotals
	################################################### */
    private String createQueryString(IndexQueryBuilder query, final ElementCategory resultType,
                                     final StandardJanusGraphTx transaction, MixedIndexType index) {
        Preconditions.checkArgument(index.getElement()==resultType,"Index is not configured for the desired result type: %s",resultType);
        final String backingIndexName = index.getBackingIndexName();
        final IndexProvider indexInformation = (IndexProvider) mixedIndexes.get(backingIndexName);

        final StringBuilder qB = new StringBuilder(query.getQuery());
        final String prefix = query.getPrefix();
        Preconditions.checkNotNull(prefix);
        //Convert query string by replacing
        int replacements = 0;
        int pos = 0;
        while (pos<qB.length()) {
            pos = qB.indexOf(prefix,pos);
            if (pos<0) break;

            final int startPos = pos;
            pos += prefix.length();
            final StringBuilder keyBuilder = new StringBuilder();
            final boolean quoteTerminated = qB.charAt(pos)=='"';
            if (quoteTerminated) pos++;
            while (pos<qB.length() &&
                (Character.isLetterOrDigit(qB.charAt(pos))
                    || (quoteTerminated && qB.charAt(pos)!='"') || qB.charAt(pos) == '*' ) ) {
                keyBuilder.append(qB.charAt(pos));
                pos++;
            }
            if (quoteTerminated) pos++;
            final int endPos = pos;
            final String keyName = keyBuilder.toString();
            Preconditions.checkArgument(StringUtils.isNotBlank(keyName),
                "Found reference to empty key at position [%s]",startPos);
            String replacement;
            if(keyName.equals("*")) {
                replacement = indexInformation.getFeatures().getWildcardField();
            }
            else if (transaction.containsRelationType(keyName)) {
                final PropertyKey key = transaction.getPropertyKey(keyName);
                Preconditions.checkNotNull(key);
                Preconditions.checkArgument(index.indexesKey(key),
                    "The used key [%s] is not indexed in the targeted index [%s]",key.name(),query.getIndex());
                replacement = key2Field(index,key);
            } else {
                Preconditions.checkArgument(query.getUnknownKeyName()!=null,
                    "Found reference to nonexistent property key in query at position [%s]: %s",startPos,keyName);
                replacement = query.getUnknownKeyName();
            }
            Preconditions.checkArgument(StringUtils.isNotBlank(replacement));

            qB.replace(startPos,endPos,replacement);
            pos = startPos+replacement.length();
            replacements++;
        }
        final String queryStr = qB.toString();
        if (replacements<=0) log.warn("Could not convert given {} index query: [{}]",resultType, query.getQuery());
        log.info("Converted query string with {} replacements: [{}] => [{}]",replacements,query.getQuery(),queryStr);
        return queryStr;
    }

    private ImmutableList<IndexQuery.OrderEntry> getOrders(IndexQueryBuilder query, final ElementCategory resultType,
                                                           final StandardJanusGraphTx transaction, MixedIndexType index){
        if (query.getOrders() == null) {
            return ImmutableList.of();
        }
        Preconditions.checkArgument(index.getElement()==resultType,"Index is not configured for the desired result type: %s",resultType);
        List<IndexQuery.OrderEntry> orderReplacement = new ArrayList<>();
        for (Parameter<Order> order: query.getOrders()) {
            if (transaction.containsRelationType(order.key())) {
                final PropertyKey key = transaction.getPropertyKey(order.key());
                Preconditions.checkNotNull(key);
                Preconditions.checkArgument(index.indexesKey(key),
                    "The used key [%s] is not indexed in the targeted index [%s]",key.name(),query.getIndex());
                orderReplacement.add(new IndexQuery.OrderEntry(key2Field(index,key), org.janusgraph.graphdb.internal.Order.convert(order.value()), key.dataType()));
            } else {
                Preconditions.checkArgument(query.getUnknownKeyName()!=null,
                    "Found reference to nonexistent property key in query orders %s", order.key());
            }
        }
        return ImmutableList.copyOf(orderReplacement);
    }

    public Stream<RawQuery.Result> executeQuery(IndexQueryBuilder query, final ElementCategory resultType,
                                                final BackendTransaction backendTx, final StandardJanusGraphTx transaction) {
        final MixedIndexType index = getMixedIndex(query.getIndex(), transaction);
        final String queryStr = createQueryString(query, resultType, transaction, index);
        ImmutableList<IndexQuery.OrderEntry> orders = getOrders(query, resultType, transaction, index);
        final RawQuery rawQuery = new RawQuery(index.getStoreName(),queryStr,orders,query.getParameters());
        if (query.hasLimit()) rawQuery.setLimit(query.getLimit());
        rawQuery.setOffset(query.getOffset());
        return backendTx.rawQuery(index.getBackingIndexName(), rawQuery).map(result ->  new RawQuery.Result(string2ElementId(result.getResult()), result.getScore()));
    }

    public Long executeTotals(IndexQueryBuilder query, final ElementCategory resultType,
                              final BackendTransaction backendTx, final StandardJanusGraphTx transaction) {
        final MixedIndexType index = getMixedIndex(query.getIndex(), transaction);
        final String queryStr = createQueryString(query, resultType, transaction, index);
        final RawQuery rawQuery = new RawQuery(index.getStoreName(),queryStr,query.getParameters());
        if (query.hasLimit()) rawQuery.setLimit(query.getLimit());
        rawQuery.setOffset(query.getOffset());
        return backendTx.totals(index.getBackingIndexName(), rawQuery);
    }

    /* ################################################
                Utility Functions
    ################################################### */

    private static MixedIndexType getMixedIndex(String indexName, StandardJanusGraphTx transaction) {
        final IndexType index = ManagementSystem.getGraphIndexDirect(indexName, transaction);
        Preconditions.checkArgument(index!=null,"Index with name [%s] is unknown or not configured properly",indexName);
        Preconditions.checkArgument(index.isMixedIndex());
        return (MixedIndexType)index;
    }

    private static String element2String(JanusGraphElement element) {
        return element2String(element.id());
    }

    private static String element2String(Object elementId) {
        Preconditions.checkArgument(elementId instanceof Long || elementId instanceof RelationIdentifier);
        if (elementId instanceof Long) return longID2Name((Long)elementId);
        else return ((RelationIdentifier) elementId).toString();
    }

    private static Object string2ElementId(String str) {
        if (str.contains(RelationIdentifier.TOSTRING_DELIMITER)) return RelationIdentifier.parse(str);
        else return name2LongID(str);
    }

    private static String key2Field(MixedIndexType index, PropertyKey key) {
        return key2Field(index.getField(key));
    }

    private static String key2Field(ParameterIndexField field) {
        assert field!=null;
        return ParameterType.MAPPED_NAME.findParameter(field.getParameters(),keyID2Name(field.getFieldKey()));
    }

    private static String keyID2Name(PropertyKey key) {
        return longID2Name(key.longId());
    }

    private static String longID2Name(long id) {
        Preconditions.checkArgument(id > 0);
        return LongEncoding.encode(id);
    }

    private static long name2LongID(String name) {
        return LongEncoding.decode(name);
    }


    private StaticBuffer getIndexKey(CompositeIndexType index, RecordEntry[] record) {
        return getIndexKey(index,IndexRecords.getValues(record));
    }

    private StaticBuffer getIndexKey(CompositeIndexType index, Object[] values) {
        final DataOutput out = serializer.getDataOutput(8*DEFAULT_OBJECT_BYTELEN + 8);
        VariableLong.writePositive(out, index.getID());
        final IndexField[] fields = index.getFieldKeys();
        Preconditions.checkArgument(fields.length>0 && fields.length==values.length);
        for (int i = 0; i < fields.length; i++) {
            final IndexField f = fields[i];
            final Object value = values[i];
            Preconditions.checkNotNull(value);
            if (InternalAttributeUtil.hasGenericDataType(f.getFieldKey())) {
                out.writeClassAndObject(value);
            } else {
                assert value.getClass().equals(f.getFieldKey().dataType()) : value.getClass() + " - " + f.getFieldKey().dataType();
                out.writeObjectNotNull(value);
            }
        }
        StaticBuffer key = out.getStaticBuffer();
        if (hashKeys) key = HashingUtil.hashPrefixKey(hashLength,key);
        return key;
    }

    public long getIndexIdFromKey(StaticBuffer key) {
        if (hashKeys) key = HashingUtil.getKey(hashLength,key);
        return VariableLong.readPositive(key.asReadBuffer());
    }

    private Entry getIndexEntry(CompositeIndexType index, RecordEntry[] record, JanusGraphElement element) {
        final DataOutput out = serializer.getDataOutput(1+8+8*record.length+4*8);
        out.putByte(FIRST_INDEX_COLUMN_BYTE);
        if (index.getCardinality()!=Cardinality.SINGLE) {
            VariableLong.writePositive(out,element.longId());
            if (index.getCardinality()!=Cardinality.SET) {
                for (final RecordEntry re : record) {
                    VariableLong.writePositive(out,re.relationId);
                }
            }
        }
        final int valuePosition=out.getPosition();
        if (element instanceof JanusGraphVertex) {
            VariableLong.writePositive(out,element.longId());
        } else {
            assert element instanceof JanusGraphRelation;
            final RelationIdentifier rid = (RelationIdentifier)element.id();
            final long[] longs = rid.getLongRepresentation();
            Preconditions.checkArgument(longs.length == 3 || longs.length == 4);
            for (final long aLong : longs) VariableLong.writePositive(out, aLong);
        }
        return new StaticArrayEntry(out.getStaticBuffer(),valuePosition);
    }

    private static RelationIdentifier bytebuffer2RelationId(ReadBuffer b) {
        long[] relationId = new long[4];
        for (int i = 0; i < 3; i++) relationId[i] = VariableLong.readPositive(b);
        if (b.hasRemaining()) relationId[3] = VariableLong.readPositive(b);
        else relationId = Arrays.copyOfRange(relationId,0,3);
        return RelationIdentifier.get(relationId);
    }


}
