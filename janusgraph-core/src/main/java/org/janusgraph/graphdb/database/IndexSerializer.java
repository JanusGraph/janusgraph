package com.thinkaurelius.titan.graphdb.database;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.schema.Parameter;
import com.thinkaurelius.titan.core.schema.SchemaStatus;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.query.graph.GraphCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.query.graph.MultiKeySliceQuery;
import com.thinkaurelius.titan.graphdb.types.ParameterType;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.indexing.*;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.diskstorage.util.HashingUtil;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.management.ManagementSystem;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeUtil;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.internal.*;
import com.thinkaurelius.titan.graphdb.query.graph.IndexQueryBuilder;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.thinkaurelius.titan.graphdb.query.condition.*;
import com.thinkaurelius.titan.graphdb.query.graph.JointIndexQuery;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.*;
import com.thinkaurelius.titan.util.encoding.LongEncoding;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_NAME_MAPPING;

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
        Preconditions.checkArgument(!ParameterType.MAPPED_NAME.hasParameter(parameters),"A field name mapping has been specified for key: %s",key);
        Preconditions.checkArgument(containsIndex(indexName),"Unknown backing index: %s",indexName);
        String fieldname = configuration.get(INDEX_NAME_MAPPING,indexName)?key.name():keyID2Name(key);
        return mixedIndexes.get(indexName).mapKey2Field(fieldname,
                new StandardKeyInformation(key,parameters));
    }

    public static void register(final MixedIndexType index, final PropertyKey key, final BackendTransaction tx) throws BackendException {
        tx.getIndexTransaction(index.getBackingIndexName()).register(index.getStoreName(), key2Field(index,key), getKeyInformation(index.getField(key)));

    }

//    public boolean supports(final String indexName, final Class<?> dataType, final Parameter[] parameters) {
//        IndexInformation indexinfo = indexes.get(indexName);
//        Preconditions.checkArgument(indexinfo != null, "Index is unknown or not configured: %s", indexName);
//        return indexinfo.supports(new StandardKeyInformation(dataType,parameters));
//    }

    public boolean supports(final MixedIndexType index, final ParameterIndexField field) {
        IndexInformation indexinfo = mixedIndexes.get(index.getBackingIndexName());
        Preconditions.checkArgument(indexinfo != null, "Index is unknown or not configured: %s", index.getBackingIndexName());
        return indexinfo.supports(getKeyInformation(field));
    }

    public boolean supports(final MixedIndexType index, final ParameterIndexField field, final TitanPredicate predicate) {
        IndexInformation indexinfo = mixedIndexes.get(index.getBackingIndexName());
        Preconditions.checkArgument(indexinfo != null, "Index is unknown or not configured: %s", index.getBackingIndexName());
        return indexinfo.supports(getKeyInformation(field),predicate);
    }

    private static StandardKeyInformation getKeyInformation(final ParameterIndexField field) {
        return new StandardKeyInformation(field.getFieldKey(),field.getParameters());
    }

    public IndexInfoRetriever getIndexInfoRetriever(StandardTitanTx tx) {
        return new IndexInfoRetriever(tx);
    }

    public static class IndexInfoRetriever implements KeyInformation.Retriever {

        private final StandardTitanTx transaction;

        private IndexInfoRetriever(StandardTitanTx tx) {
            Preconditions.checkNotNull(tx);
            transaction=tx;
        }

        @Override
        public KeyInformation.IndexRetriever get(final String index) {
            return new KeyInformation.IndexRetriever() {

                Map<String,KeyInformation.StoreRetriever> indexes = new ConcurrentHashMap<String, KeyInformation.StoreRetriever>();

                @Override
                public KeyInformation get(String store, String key) {
                    return get(store).get(key);
                }

                @Override
                public KeyInformation.StoreRetriever get(final String store) {
                    if (indexes.get(store)==null) {
                        Preconditions.checkState(transaction!=null,"Retriever has not been initialized");
                        final MixedIndexType extIndex = getMixedIndex(store, transaction);
                        assert extIndex.getBackingIndexName().equals(index);
                        ImmutableMap.Builder<String,KeyInformation> b = ImmutableMap.builder();
                        for (ParameterIndexField field : extIndex.getFieldKeys()) b.put(key2Field(field),getKeyInformation(field));
                        final ImmutableMap<String,KeyInformation> infoMap = b.build();
                        KeyInformation.StoreRetriever storeRetriever = new KeyInformation.StoreRetriever() {

                            @Override
                            public KeyInformation get(String key) {
                                return infoMap.get(key);
                            }
                        };
                        indexes.put(store,storeRetriever);
                    }
                    return indexes.get(store);
                }

            };
        }
    }

    /* ################################################
               Index Updates
    ################################################### */

    public static class IndexUpdate<K,E> {

        private enum Type { ADD, DELETE };

        private final IndexType index;
        private final Type mutationType;
        private final K key;
        private final E entry;
        private final TitanElement element;

        private IndexUpdate(IndexType index, Type mutationType, K key, E entry, TitanElement element) {
            assert index!=null && mutationType!=null && key!=null && entry!=null && element!=null;
            assert !index.isCompositeIndex() || (key instanceof StaticBuffer && entry instanceof Entry);
            assert !index.isMixedIndex() || (key instanceof String && entry instanceof IndexEntry);
            this.index = index;
            this.mutationType = mutationType;
            this.key = key;
            this.entry = entry;
            this.element = element;
        }

        public TitanElement getElement() {
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
            return new HashCodeBuilder().append(index).append(mutationType).append(key).append(entry).toHashCode();
        }

        @Override
        public boolean equals(Object other) {
            if (this==other) return true;
            else if (other==null || !(other instanceof IndexUpdate)) return false;
            IndexUpdate oth = (IndexUpdate)other;
            return index.equals(oth.index) && mutationType==oth.mutationType && key.equals(oth.key) && entry.equals(oth.entry);
        }
    }

    private static final IndexUpdate.Type getUpateType(InternalRelation relation) {
        assert relation.isNew() || relation.isRemoved();
        return (relation.isNew()? IndexUpdate.Type.ADD : IndexUpdate.Type.DELETE);
    }

    private static boolean indexAppliesTo(IndexType index, TitanElement element) {
        return index.getElement().isInstance(element) &&
                (!(index instanceof CompositeIndexType) || ((CompositeIndexType)index).getStatus()!=SchemaStatus.DISABLED) &&
                (!index.hasSchemaTypeConstraint() ||
                        index.getElement().matchesConstraint(index.getSchemaTypeConstraint(),element));
    }

    public Collection<IndexUpdate> getIndexUpdates(InternalRelation relation) {
        assert relation.isNew() || relation.isRemoved();
        Set<IndexUpdate> updates = Sets.newHashSet();
        IndexUpdate.Type updateType = getUpateType(relation);
        int ttl = updateType==IndexUpdate.Type.ADD?StandardTitanGraph.getTTL(relation):0;
        for (RelationType type : relation.getPropertyKeysDirect()) {
            if (!(type instanceof PropertyKey)) continue;
            PropertyKey key = (PropertyKey)type;
            for (IndexType index : ((InternalRelationType)key).getKeyIndexes()) {
                if (!indexAppliesTo(index,relation)) continue;
                IndexUpdate update;
                if (index instanceof CompositeIndexType) {
                    CompositeIndexType iIndex= (CompositeIndexType) index;
                    RecordEntry[] record = indexMatch(relation, iIndex);
                    if (record==null) continue;
                    update = new IndexUpdate<StaticBuffer,Entry>(iIndex,updateType,getIndexKey(iIndex,record),getIndexEntry(iIndex,record,relation), relation);
                } else {
                    assert relation.valueOrNull(key)!=null;
                    if (((MixedIndexType)index).getField(key).getStatus()== SchemaStatus.DISABLED) continue;
                    update = getMixedIndexUpdate(relation, key, relation.valueOrNull(key), (MixedIndexType) index, updateType);
                }
                if (ttl>0) update.setTTL(ttl);
                updates.add(update);
            }
        }
        return updates;
    }

    private static PropertyKey[] getKeysOfRecords(RecordEntry[] record) {
        PropertyKey[] keys = new PropertyKey[record.length];
        for (int i=0;i<record.length;i++) keys[i]=record[i].key;
        return keys;
    }

    private static int getIndexTTL(InternalVertex vertex, PropertyKey... keys) {
        int ttl = StandardTitanGraph.getTTL(vertex);
        for (int i=0;i<keys.length;i++) {
            PropertyKey key = keys[i];
            int kttl = ((InternalRelationType)key).getTTL();
            if (kttl>0 && (kttl<ttl || ttl<=0)) ttl=kttl;
        }
        return ttl;
    }

    public Collection<IndexUpdate> getIndexUpdates(InternalVertex vertex, Collection<InternalRelation> updatedProperties) {
        if (updatedProperties.isEmpty()) return Collections.EMPTY_LIST;
        Set<IndexUpdate> updates = Sets.newHashSet();

        for (InternalRelation rel : updatedProperties) {
            assert rel.isProperty();
            TitanVertexProperty p = (TitanVertexProperty)rel;
            assert rel.isNew() || rel.isRemoved(); assert rel.getVertex(0).equals(vertex);
            IndexUpdate.Type updateType = getUpateType(rel);
            for (IndexType index : ((InternalRelationType)p.propertyKey()).getKeyIndexes()) {
                if (!indexAppliesTo(index,vertex)) continue;
                if (index.isCompositeIndex()) { //Gather composite indexes
                    CompositeIndexType cIndex = (CompositeIndexType)index;
                    IndexRecords updateRecords = indexMatches(vertex,cIndex,updateType==IndexUpdate.Type.DELETE,p.propertyKey(),new RecordEntry(p));
                    for (RecordEntry[] record : updateRecords) {
                        IndexUpdate update = new IndexUpdate<StaticBuffer,Entry>(cIndex,updateType,getIndexKey(cIndex,record),getIndexEntry(cIndex,record,vertex), vertex);
                        int ttl = getIndexTTL(vertex,getKeysOfRecords(record));
                        if (ttl>0 && updateType== IndexUpdate.Type.ADD) update.setTTL(ttl);
                        updates.add(update);
                    }
                } else { //Update mixed indexes
                    if (((MixedIndexType)index).getField(p.propertyKey()).getStatus()== SchemaStatus.DISABLED) continue;
                    IndexUpdate update = getMixedIndexUpdate(vertex, p.propertyKey(), p.value(), (MixedIndexType) index, updateType);
                    int ttl = getIndexTTL(vertex,p.propertyKey());
                    if (ttl>0 && updateType== IndexUpdate.Type.ADD) update.setTTL(ttl);
                    updates.add(update);
                }
            }
        }
        return updates;
    }

    private IndexUpdate<String,IndexEntry> getMixedIndexUpdate(TitanElement element, PropertyKey key, Object value,
                                                               MixedIndexType index, IndexUpdate.Type updateType)  {
        return new IndexUpdate<String,IndexEntry>(index,updateType,element2String(element),new IndexEntry(key2Field(index.getField(key)), value), element);
    }

    public void reindexElement(TitanElement element, MixedIndexType index, Map<String,Map<String,List<IndexEntry>>> documentsPerStore) {
        if (!indexAppliesTo(index,element)) return;
        List<IndexEntry> entries = Lists.newArrayList();
        for (ParameterIndexField field: index.getFieldKeys()) {
            PropertyKey key = field.getFieldKey();
            if (field.getStatus()==SchemaStatus.DISABLED) continue;
            if (element.properties(key.name()).hasNext()) {
                element.values(key.name()).forEachRemaining(value->entries.add(new IndexEntry(key2Field(field), value)));
            }
        }
        Map<String,List<IndexEntry>> documents = documentsPerStore.get(index.getStoreName());
        if (documents==null) {
            documents = Maps.newHashMap();
            documentsPerStore.put(index.getStoreName(),documents);
        }
        getDocuments(documentsPerStore,index).put(element2String(element),entries);
    }

    private Map<String,List<IndexEntry>> getDocuments(Map<String,Map<String,List<IndexEntry>>> documentsPerStore, MixedIndexType index) {
        Map<String,List<IndexEntry>> documents = documentsPerStore.get(index.getStoreName());
        if (documents==null) {
            documents = Maps.newHashMap();
            documentsPerStore.put(index.getStoreName(),documents);
        }
        return documents;
    }

    public void removeElement(Object elementId, MixedIndexType index, Map<String,Map<String,List<IndexEntry>>> documentsPerStore) {
        Preconditions.checkArgument((index.getElement()==ElementCategory.VERTEX && elementId instanceof Long) ||
                (index.getElement().isRelation() && elementId instanceof RelationIdentifier),"Invalid element id [%s] provided for index: %s",elementId,index);
        getDocuments(documentsPerStore,index).put(element2String(elementId),Lists.<IndexEntry>newArrayList());
    }

    public Set<IndexUpdate<StaticBuffer,Entry>> reindexElement(TitanElement element, CompositeIndexType index) {
        Set<IndexUpdate<StaticBuffer,Entry>> indexEntries = Sets.newHashSet();
        if (!indexAppliesTo(index,element)) return indexEntries;
        Iterable<RecordEntry[]> records;
        if (element instanceof TitanVertex) records = indexMatches((TitanVertex)element,index);
        else {
            assert element instanceof TitanRelation;
            records = Collections.EMPTY_LIST;
            RecordEntry[] record = indexMatch((TitanRelation)element,index);
            if (record!=null) records = ImmutableList.of(record);
        }
        for (RecordEntry[] record : records) {
            indexEntries.add(new IndexUpdate<StaticBuffer,Entry>(index, IndexUpdate.Type.ADD,getIndexKey(index,record),getIndexEntry(index,record,element), element));
        }
        return indexEntries;
    }

    public static RecordEntry[] indexMatch(TitanRelation relation, CompositeIndexType index) {
        IndexField[] fields = index.getFieldKeys();
        RecordEntry[] match = new RecordEntry[fields.length];
        for (int i = 0; i <fields.length; i++) {
            IndexField f = fields[i];
            Object value = relation.valueOrNull(f.getFieldKey());
            if (value==null) return null; //No match
            match[i] = new RecordEntry(relation.longId(),value,f.getFieldKey());
        }
        return match;
    }

    public static class IndexRecords extends ArrayList<RecordEntry[]> {

        public boolean add(RecordEntry[] record) {
            return super.add(Arrays.copyOf(record,record.length));
        }

        public Iterable<Object[]> getRecordValues() {
            return Iterables.transform(this, new Function<RecordEntry[], Object[]>() {
                @Nullable
                @Override
                public Object[] apply(@Nullable RecordEntry[] record) {
                    return getValues(record);
                }
            });
        }

        private static Object[] getValues(RecordEntry[] record) {
            Object[] values = new Object[record.length];
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

        private RecordEntry(TitanVertexProperty property) {
            this(property.longId(),property.value(),property.propertyKey());
        }
    }

    public static IndexRecords indexMatches(TitanVertex vertex, CompositeIndexType index) {
        return indexMatches(vertex,index,null,null);
    }

    public static IndexRecords indexMatches(TitanVertex vertex, CompositeIndexType index,
                                            PropertyKey replaceKey, Object replaceValue) {
        IndexRecords matches = new IndexRecords();
        IndexField[] fields = index.getFieldKeys();
        if (indexAppliesTo(index,vertex)) {
            indexMatches(vertex,new RecordEntry[fields.length],matches,fields,0,false,
                                            replaceKey,new RecordEntry(0,replaceValue,replaceKey));
        }
        return matches;
    }

    private static IndexRecords indexMatches(TitanVertex vertex, CompositeIndexType index,
                                              boolean onlyLoaded, PropertyKey replaceKey, RecordEntry replaceValue) {
        IndexRecords matches = new IndexRecords();
        IndexField[] fields = index.getFieldKeys();
        indexMatches(vertex,new RecordEntry[fields.length],matches,fields,0,onlyLoaded,replaceKey,replaceValue);
        return matches;
    }

    private static void indexMatches(TitanVertex vertex, RecordEntry[] current, IndexRecords matches,
                                     IndexField[] fields, int pos,
                                     boolean onlyLoaded, PropertyKey replaceKey, RecordEntry replaceValue) {
        if (pos>= fields.length) {
            matches.add(current);
            return;
        }

        PropertyKey key = fields[pos].getFieldKey();

        List<RecordEntry> values;
        if (key.equals(replaceKey)) {
            values = ImmutableList.of(replaceValue);
        } else {
            values = new ArrayList<RecordEntry>();
            Iterable<TitanVertexProperty> props;
            if (onlyLoaded ||
                    (!vertex.isNew() && IDManager.VertexIDType.PartitionedVertex.is(vertex.longId()))) {
                //going through transaction so we can query deleted vertices
                VertexCentricQueryBuilder qb = ((InternalVertex)vertex).tx().query(vertex);
                qb.noPartitionRestriction().type(key);
                if (onlyLoaded) qb.queryOnlyLoaded();
                props = qb.properties();
            } else {
                props = vertex.query().keys(key.name()).properties();
            }
            for (TitanVertexProperty p : props) {
                assert !onlyLoaded || p.isLoaded() || p.isRemoved();
                assert key.dataType().equals(p.value().getClass()) : key + " -> " + p;
                values.add(new RecordEntry(p));
            }
        }
        for (RecordEntry value : values) {
            current[pos]=value;
            indexMatches(vertex,current,matches,fields,pos+1,onlyLoaded,replaceKey,replaceValue);
        }
    }


    /* ################################################
                Querying
    ################################################### */

    public List<Object> query(final JointIndexQuery.Subquery query, final BackendTransaction tx) {
        IndexType index = query.getIndex();
        if (index.isCompositeIndex()) {
            MultiKeySliceQuery sq = query.getCompositeQuery();
            List<EntryList> rs = sq.execute(tx);
            List<Object> results = new ArrayList<Object>(rs.get(0).size());
            for (EntryList r : rs) {
                for (java.util.Iterator<Entry> iterator = r.reuseIterator(); iterator.hasNext(); ) {
                    Entry entry = iterator.next();
                    ReadBuffer entryValue = entry.asReadBuffer();
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
            return results;
        } else {
            List<String> r = tx.indexQuery(((MixedIndexType) index).getBackingIndexName(), query.getMixedQuery());
            List<Object> result = new ArrayList<Object>(r.size());
            for (String id : r) result.add(string2ElementId(id));
            return result;
        }
    }

    public MultiKeySliceQuery getQuery(final CompositeIndexType index, List<Object[]> values) {
        List<KeySliceQuery> ksqs = new ArrayList<KeySliceQuery>(values.size());
        for (Object[] value : values) {
            ksqs.add(new KeySliceQuery(getIndexKey(index,value), BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(1)));
        }
        return new MultiKeySliceQuery(ksqs);
    }

    public IndexQuery getQuery(final MixedIndexType index, final Condition condition, final OrderList orders) {
        Condition newCondition = ConditionUtil.literalTransformation(condition,
                new Function<Condition<TitanElement>, Condition<TitanElement>>() {
                    @Nullable
                    @Override
                    public Condition<TitanElement> apply(@Nullable Condition<TitanElement> condition) {
                        Preconditions.checkArgument(condition instanceof PredicateCondition);
                        PredicateCondition pc = (PredicateCondition) condition;
                        PropertyKey key = (PropertyKey) pc.getKey();
                        return new PredicateCondition<String, TitanElement>(key2Field(index,key), pc.getPredicate(), pc.getValue());
                    }
                });
        ImmutableList<IndexQuery.OrderEntry> newOrders = IndexQuery.NO_ORDER;
        if (!orders.isEmpty() && GraphCentricQueryBuilder.indexCoversOrder(index,orders)) {
            ImmutableList.Builder<IndexQuery.OrderEntry> lb = ImmutableList.builder();
            for (int i = 0; i < orders.size(); i++) {
                lb.add(new IndexQuery.OrderEntry(key2Field(index,orders.getKey(i)), orders.getOrder(i), orders.getKey(i).dataType()));
            }
            newOrders = lb.build();
        }
        return new IndexQuery(index.getStoreName(), newCondition, newOrders);
    }
//
//
//
//    public IndexQuery getQuery(String index, final ElementCategory resultType, final Condition condition, final OrderList orders) {
//        if (isStandardIndex(index)) {
//            Preconditions.checkArgument(orders.isEmpty());
//            return new IndexQuery(getStoreName(resultType), condition, IndexQuery.NO_ORDER);
//        } else {
//            Condition newCondition = ConditionUtil.literalTransformation(condition,
//                    new Function<Condition<TitanElement>, Condition<TitanElement>>() {
//                        @Nullable
//                        @Override
//                        public Condition<TitanElement> apply(@Nullable Condition<TitanElement> condition) {
//                            Preconditions.checkArgument(condition instanceof PredicateCondition);
//                            PredicateCondition pc = (PredicateCondition) condition;
//                            TitanKey key = (TitanKey) pc.getKey();
//                            return new PredicateCondition<String, TitanElement>(key2Field(key), pc.getPredicate(), pc.getValue());
//                        }
//                    });
//            ImmutableList<IndexQuery.OrderEntry> newOrders = IndexQuery.NO_ORDER;
//            if (!orders.isEmpty()) {
//                ImmutableList.Builder<IndexQuery.OrderEntry> lb = ImmutableList.builder();
//                for (int i = 0; i < orders.size(); i++) {
//                    lb.add(new IndexQuery.OrderEntry(key2Field(orders.getKey(i)), orders.getOrder(i), orders.getKey(i).getDataType()));
//                }
//                newOrders = lb.build();
//            }
//            return new IndexQuery(getStoreName(resultType), newCondition, newOrders);
//        }
//    }

    public Iterable<RawQuery.Result> executeQuery(IndexQueryBuilder query, final ElementCategory resultType,
                                                  final BackendTransaction backendTx, final StandardTitanTx transaction) {
        MixedIndexType index = getMixedIndex(query.getIndex(), transaction);
        Preconditions.checkArgument(index.getElement()==resultType,"Index is not configured for the desired result type: %s",resultType);
        String backingIndexName = index.getBackingIndexName();
        IndexProvider indexInformation = (IndexProvider) mixedIndexes.get(backingIndexName);

        StringBuffer qB = new StringBuffer(query.getQuery());
        final String prefix = query.getPrefix();
        Preconditions.checkNotNull(prefix);
        //Convert query string by replacing
        int replacements = 0;
        int pos = 0;
        while (pos<qB.length()) {
            pos = qB.indexOf(prefix,pos);
            if (pos<0) break;

            int startPos = pos;
            pos += prefix.length();
            StringBuilder keyBuilder = new StringBuilder();
            boolean quoteTerminated = qB.charAt(pos)=='"';
            if (quoteTerminated) pos++;
            while (pos<qB.length() &&
                    (Character.isLetterOrDigit(qB.charAt(pos))
                            || (quoteTerminated && qB.charAt(pos)!='"') || qB.charAt(pos) == '*' ) ) {
                keyBuilder.append(qB.charAt(pos));
                pos++;
            }
            if (quoteTerminated) pos++;
            int endPos = pos;
            String keyname = keyBuilder.toString();
            Preconditions.checkArgument(StringUtils.isNotBlank(keyname),
                    "Found reference to empty key at position [%s]",startPos);
            String replacement;
            if(keyname.equals("*")) {
                replacement = indexInformation.getFeatures().getWildcardField();
            }
            else if (transaction.containsRelationType(keyname)) {
                PropertyKey key = transaction.getPropertyKey(keyname);
                Preconditions.checkNotNull(key);
                Preconditions.checkArgument(index.indexesKey(key),
                        "The used key [%s] is not indexed in the targeted index [%s]",key.name(),query.getIndex());
                replacement = key2Field(index,key);
            } else {
                Preconditions.checkArgument(query.getUnknownKeyName()!=null,
                        "Found reference to non-existant property key in query at position [%s]: %s",startPos,keyname);
                replacement = query.getUnknownKeyName();
            }
            Preconditions.checkArgument(StringUtils.isNotBlank(replacement));

            qB.replace(startPos,endPos,replacement);
            pos = startPos+replacement.length();
            replacements++;
        }

        String queryStr = qB.toString();
        if (replacements<=0) log.warn("Could not convert given {} index query: [{}]",resultType, query.getQuery());
        log.info("Converted query string with {} replacements: [{}] => [{}]",replacements,query.getQuery(),queryStr);
        RawQuery rawQuery=new RawQuery(index.getStoreName(),queryStr,query.getParameters());
        if (query.hasLimit()) rawQuery.setLimit(query.getLimit());
        rawQuery.setOffset(query.getOffset());
        return Iterables.transform(backendTx.rawQuery(index.getBackingIndexName(), rawQuery), new Function<RawQuery.Result<String>, RawQuery.Result>() {
            @Nullable
            @Override
            public RawQuery.Result apply(@Nullable RawQuery.Result<String> result) {
                return new RawQuery.Result(string2ElementId(result.getResult()), result.getScore());
            }
        });
    }


    /* ################################################
                Utility Functions
    ################################################### */

    private static final MixedIndexType getMixedIndex(String indexName, StandardTitanTx transaction) {
        IndexType index = ManagementSystem.getGraphIndexDirect(indexName, transaction);
        Preconditions.checkArgument(index!=null,"Index with name [%s] is unknown or not configured properly",indexName);
        Preconditions.checkArgument(index.isMixedIndex());
        return (MixedIndexType)index;
    }

    private static final String element2String(TitanElement element) {
        return element2String(element.id());
    }

    private static final String element2String(Object elementId) {
        Preconditions.checkArgument(elementId instanceof Long || elementId instanceof RelationIdentifier);
        if (elementId instanceof Long) return longID2Name((Long)elementId);
        else return ((RelationIdentifier) elementId).toString();
    }

    private static final Object string2ElementId(String str) {
        if (str.contains(RelationIdentifier.TOSTRING_DELIMITER)) return RelationIdentifier.parse(str);
        else return name2LongID(str);
    }

    private static final String key2Field(MixedIndexType index, PropertyKey key) {
        return key2Field(index.getField(key));
    }

    private static final String key2Field(ParameterIndexField field) {
        assert field!=null;
        return ParameterType.MAPPED_NAME.findParameter(field.getParameters(),keyID2Name(field.getFieldKey()));
    }

    private static final String keyID2Name(PropertyKey key) {
        return longID2Name(key.longId());
    }

    private static final String longID2Name(long id) {
        Preconditions.checkArgument(id > 0);
        return LongEncoding.encode(id);
    }

    private static final long name2LongID(String name) {
        return LongEncoding.decode(name);
    }


    private final StaticBuffer getIndexKey(CompositeIndexType index, RecordEntry[] record) {
        return getIndexKey(index,IndexRecords.getValues(record));
    }

    private final StaticBuffer getIndexKey(CompositeIndexType index, Object[] values) {
        DataOutput out = serializer.getDataOutput(8*DEFAULT_OBJECT_BYTELEN + 8);
        VariableLong.writePositive(out, index.getID());
        IndexField[] fields = index.getFieldKeys();
        Preconditions.checkArgument(fields.length>0 && fields.length==values.length);
        for (int i = 0; i < fields.length; i++) {
            IndexField f = fields[i];
            Object value = values[i];
            Preconditions.checkNotNull(value);
            if (AttributeUtil.hasGenericDataType(f.getFieldKey())) {
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

    private final Entry getIndexEntry(CompositeIndexType index, RecordEntry[] record, TitanElement element) {
        DataOutput out = serializer.getDataOutput(1+8+8*record.length+4*8);
        out.putByte(FIRST_INDEX_COLUMN_BYTE);
        if (index.getCardinality()!=Cardinality.SINGLE) {
            VariableLong.writePositive(out,element.longId());
            if (index.getCardinality()!=Cardinality.SET) {
                for (RecordEntry re : record) {
                    VariableLong.writePositive(out,re.relationId);
                }
            }
        }
        int valuePosition=out.getPosition();
        if (element instanceof TitanVertex) {
            VariableLong.writePositive(out,element.longId());
        } else {
            assert element instanceof TitanRelation;
            RelationIdentifier rid = (RelationIdentifier)element.id();
            long[] longs = rid.getLongRepresentation();
            Preconditions.checkArgument(longs.length == 3 || longs.length == 4);
            for (int i = 0; i < longs.length; i++) VariableLong.writePositive(out, longs[i]);
        }
        return new StaticArrayEntry(out.getStaticBuffer(),valuePosition);
    }

    private static final RelationIdentifier bytebuffer2RelationId(ReadBuffer b) {
        long[] relationId = new long[4];
        for (int i = 0; i < 3; i++) relationId[i] = VariableLong.readPositive(b);
        if (b.hasRemaining()) relationId[3] = VariableLong.readPositive(b);
        else relationId = Arrays.copyOfRange(relationId,0,3);
        return RelationIdentifier.get(relationId);
    }


}
