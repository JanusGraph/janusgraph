package com.thinkaurelius.titan.graphdb.database;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.indexing.*;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
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
import com.thinkaurelius.titan.graphdb.types.indextype.ExternalIndexTypeWrapper;
import com.thinkaurelius.titan.util.encoding.LongEncoding;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IndexSerializer {

    private static final Logger log = LoggerFactory.getLogger(IndexSerializer.class);

    private static final int DEFAULT_OBJECT_BYTELEN = 30;
    private static final byte FIRST_INDEX_COLUMN_BYTE = 0;

    private final Serializer serializer;
    private final Map<String, ? extends IndexInformation> indexes;

    public IndexSerializer(Serializer serializer, Map<String, ? extends IndexInformation> indexes) {
        this.serializer = serializer;
        this.indexes = indexes;
    }


    /* ################################################
               Index Information
    ################################################### */

//    public boolean supports(final String indexName, final Class<?> dataType, final Parameter[] parameters) {
//        IndexInformation indexinfo = indexes.get(indexName);
//        Preconditions.checkArgument(indexinfo != null, "Index is unknown or not configured: %s", indexName);
//        return indexinfo.supports(new StandardKeyInformation(dataType,parameters));
//    }

    public boolean supports(final ExternalIndexType index, final ParameterIndexField field, final TitanPredicate predicate) {
        IndexInformation indexinfo = indexes.get(index.getIndexName());
        Preconditions.checkArgument(indexinfo != null, "Index is unknown or not configured: %s", index.getIndexName());
        return indexinfo.supports(getKeyInformation(field),predicate);
    }

    private static StandardKeyInformation getKeyInformation(final ParameterIndexField field) {
        return new StandardKeyInformation(field.getFieldKey().getDataType(),field.getParameters());
    }

    public IndexInfoRetriever getIndexInfoRetriever() {
        return new IndexInfoRetriever();
    }

    public static class IndexInfoRetriever implements KeyInformation.Retriever {

        private StandardTitanTx transaction;

        private IndexInfoRetriever() {}

        public void setTransaction(StandardTitanTx tx) {
            Preconditions.checkNotNull(tx);
            Preconditions.checkArgument(transaction==null);
            transaction=tx;
        }

        @Override
        public KeyInformation.IndexRetriever get(final String index) {
            return new KeyInformation.IndexRetriever() {

                EnumMap<ElementCategory,KeyInformation.StoreRetriever> indexes = new EnumMap<ElementCategory, KeyInformation.StoreRetriever>(ElementCategory.class);

                @Override
                public KeyInformation get(String store, String key) {
                    return get(store).get(key);
                }

                @Override
                public KeyInformation.StoreRetriever get(final String store) {
                    final ElementCategory elementCategory = getElementCategory(store);
                    if (indexes.get(elementCategory)==null) {
                        Preconditions.checkState(transaction!=null,"Retriever has not been initialized");
                        final ExternalIndexType extIndex = getExternalIndex(index,elementCategory,transaction);
                        ImmutableMap.Builder<String,KeyInformation> b = ImmutableMap.builder();
                        for (ParameterIndexField field : extIndex.getFields()) b.put(key2Field(field),getKeyInformation(field));
                        final ImmutableMap<String,KeyInformation> infoMap = b.build();
                        KeyInformation.StoreRetriever storeRetriever = new KeyInformation.StoreRetriever() {

                            @Override
                            public KeyInformation get(String key) {
                                return infoMap.get(key);
                            }
                        };
                        indexes.put(elementCategory,storeRetriever);
                    }
                    return indexes.get(elementCategory);
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
            assert !index.isInternalIndex() || (key instanceof StaticBuffer && entry instanceof Entry);
            assert !index.isExternalIndex() || (key instanceof String && entry instanceof IndexEntry);
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

        public boolean isInternalIndex() {
            return index.isInternalIndex();
        }

        public boolean isExternalIndex() {
            return index.isExternalIndex();
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

    public Collection<IndexUpdate> getIndexUpdates(InternalRelation relation) {
        assert relation.isNew() || relation.isRemoved();
        Set<IndexUpdate> updates = Sets.newHashSet();
        IndexUpdate.Type updateType = getUpateType(relation);
        for (TitanType type : relation.getPropertyKeysDirect()) {
            if (!(type instanceof TitanKey)) continue;
            TitanKey key = (TitanKey)type;
            for (IndexType index : ((InternalRelationType)key).getKeyIndexes()) {
                if (index instanceof InternalIndexType) {
                    InternalIndexType iIndex= (InternalIndexType) index;
                    RecordEntry[] record = indexMatch(relation, iIndex);
                    if (record==null) continue;
                    updates.add(new IndexUpdate<StaticBuffer,Entry>(iIndex,updateType,getIndexKey(iIndex,record),getIndexEntry(iIndex,record,relation), relation));
                } else {
                    assert relation.getProperty(key)!=null;
                    updates.add(getExternalIndexUpdate(relation,key,relation.getProperty(key),(ExternalIndexType)index,updateType));
                }
            }
        }
        return updates;
    }

    public Collection<IndexUpdate> getIndexUpdates(InternalVertex vertex, Collection<InternalRelation> updatedRelations) {
        if (updatedRelations.isEmpty()) return Collections.EMPTY_LIST;
        Set<IndexUpdate> updates = Sets.newHashSet();

        for (InternalRelation rel : updatedRelations) {
            if (!rel.isProperty()) continue;
            TitanProperty p = (TitanProperty)rel;
            assert rel.isNew() || rel.isRemoved(); assert rel.getVertex(0).equals(vertex);
            IndexUpdate.Type updateType = getUpateType(rel);
            for (IndexType index : ((InternalRelationType)p.getPropertyKey()).getKeyIndexes()) {
                if (index.isInternalIndex()) { //Gather internal indexes
                    InternalIndexType iIndex = (InternalIndexType)index;
                    IndexRecords updateRecords = indexMatches(vertex,iIndex,updateType==IndexUpdate.Type.DELETE,p.getPropertyKey(),new RecordEntry(p.getID(),p.getValue()));
                    for (RecordEntry[] record : updateRecords) {
                        updates.add(new IndexUpdate<StaticBuffer,Entry>(iIndex,updateType,getIndexKey(iIndex,record),getIndexEntry(iIndex,record,vertex), vertex));
                    }
                } else { //Update external indexes
                    updates.add(getExternalIndexUpdate(vertex,p.getPropertyKey(),p.getValue(),(ExternalIndexType)index,updateType));
                }
            }
        }
        return updates;
    }

    private IndexUpdate<String,IndexEntry> getExternalIndexUpdate(TitanElement element, TitanKey key, Object value,
                                                       ExternalIndexType index, IndexUpdate.Type updateType)  {
        return new IndexUpdate<String,IndexEntry>(index,updateType,element2String(element),new IndexEntry(key2Field(index.getField(key)), value), element);
    }

    public static RecordEntry[] indexMatch(TitanRelation element, InternalIndexType index) {
        IndexField[] fields = index.getFields();
        RecordEntry[] match = new RecordEntry[fields.length];
        for (int i = 0; i <fields.length; i++) {
            IndexField f = fields[i];
            Object value = element.getProperty(f.getFieldKey());
            if (value==null) return null; //No match
            match[i] = new RecordEntry(element.getID(),value);
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

        private RecordEntry(long relationId, Object value) {
            this.relationId = relationId;
            this.value = value;
        }
    }

    public static IndexRecords indexMatches(TitanVertex vertex, InternalIndexType index,
                                            TitanKey replaceKey, Object replaceValue) {
        IndexRecords matches = new IndexRecords();
        IndexField[] fields = index.getFields();
        indexMatches(vertex,new RecordEntry[fields.length],matches,fields,0,false,
                                            replaceKey,new RecordEntry(0,replaceValue));
        return matches;
    }

    private static IndexRecords indexMatches(TitanVertex vertex, InternalIndexType index,
                                              boolean onlyLoaded, TitanKey replaceKey, RecordEntry replaceValue) {
        IndexRecords matches = new IndexRecords();
        IndexField[] fields = index.getFields();
        indexMatches(vertex,new RecordEntry[fields.length],matches,fields,0,onlyLoaded,replaceKey,replaceValue);
        return matches;
    }

    private static void indexMatches(TitanVertex vertex, RecordEntry[] current, IndexRecords matches,
                                     IndexField[] fields, int pos,
                                     boolean onlyLoaded, TitanKey replaceKey, RecordEntry replaceValue) {
        if (pos>= fields.length) {
            matches.add(current);
            return;
        }

        TitanKey key = fields[pos].getFieldKey();

        List<RecordEntry> values;
        if (key.equals(replaceKey)) {
            values = ImmutableList.of(replaceValue);
        } else {
            values = new ArrayList<RecordEntry>();
            VertexCentricQueryBuilder qb = ((VertexCentricQueryBuilder)vertex.query()).type(key);
            if (onlyLoaded) qb.queryOnlyLoaded();
            for (TitanProperty p : qb.properties()) {
                assert p.isNew() || p.isLoaded(); assert !onlyLoaded || p.isLoaded();
                values.add(new RecordEntry(p.getID(),p.getValue()));
            }
        }
        for (RecordEntry value : values) {
            current[pos]=value;
            indexMatches(vertex,current,matches,fields,pos+1,onlyLoaded,replaceKey,replaceValue);
        }
    }




    ////////////////////////////////////////







//    private void removeKeyValue(TitanElement element, TitanKey key, String index, BackendTransaction tx) {
//        Preconditions.checkArgument(key.isUnique(Direction.OUT), "Only out-unique properties are supported by index [%s]", index);
//        tx.getIndexTransactionHandle(index).delete(getStoreName(element), element2String(element), key2Field(key), element.isRemoved());
//    }
//
//
//
//    public void newPropertyKey(TitanKey key, BackendTransaction tx) throws StorageException {
//        for (String index : key.getIndexes(Vertex.class)) {
//            if (!index.equals(Titan.Token.STANDARD_INDEX))
//                tx.getIndexTransactionHandle(index).register(ElementCategory.VERTEX.getName(), key2Field(key), getKeyInformation(key,Vertex.class,index));
//        }
//        for (String index : key.getIndexes(Edge.class)) {
//            if (!index.equals(Titan.Token.STANDARD_INDEX))
//                tx.getIndexTransactionHandle(index).register(ElementCategory.EDGE.getName(), key2Field(key), getKeyInformation(key, Edge.class, index));
//        }
//    }
//
//    public void addProperty(TitanProperty prop, BackendTransaction tx) throws StorageException {
//        TitanKey key = prop.getPropertyKey();
//        for (String index : key.getIndexes(Vertex.class)) {
//            if (index.equals(Titan.Token.STANDARD_INDEX)) {
//                tx.mutateVertexIndex(getIndexKey(prop.getValue()),
//                        Lists.newArrayList(StaticArrayEntry.of(getIndexColumn(key, prop), getIndexValue(prop))), NO_DELETIONS);
//            } else {
//                addKeyValue(prop.getVertex(), key, prop.getValue(), index, tx);
//            }
//        }
//    }
//
//    public void removeProperty(TitanProperty prop, BackendTransaction tx) throws StorageException {
//        TitanKey key = prop.getPropertyKey();
//        for (String index : key.getIndexes(Vertex.class)) {
//            if (index.equals(Titan.Token.STANDARD_INDEX)) {
//                tx.mutateVertexIndex(getIndexKey(prop.getValue()), NO_ADDITIONS,
//                        Lists.newArrayList(getIndexColumn(key, prop)));
//            } else {
//                removeKeyValue(prop.getVertex(), key, index, tx);
//            }
//        }
//    }
//
//    public void lockKeyedProperty(TitanProperty prop, BackendTransaction tx) throws StorageException {
//        TitanKey key = prop.getPropertyKey();
//        if (key.isUnique(Direction.IN) && ((InternalRelationType) key).uniqueLock(Direction.IN)) {
//            Preconditions.checkArgument(key.hasIndex(Titan.Token.STANDARD_INDEX, Vertex.class), "Standard Index needs to be created for property to be declared unique [%s]", key.getName());
//            Preconditions.checkArgument(prop.isNew() || prop.isRemoved());
//            tx.acquireVertexIndexLock(getIndexKey(prop.getValue()), getIndexColumn(key, prop), prop.isNew() ? null : getIndexValue(prop));
//        }
//    }
//
//    public void addEdge(InternalRelation relation, BackendTransaction tx) throws StorageException {
//        Preconditions.checkArgument(relation instanceof TitanEdge, "Only edges can be indexed for now");
//        for (TitanType type : relation.getPropertyKeysDirect()) {
//            if (type instanceof TitanKey) {
//                TitanKey key = (TitanKey) type;
//                for (String index : key.getIndexes(Edge.class)) {
//                    Object value = relation.getPropertyDirect(key);
//                    if (index.equals(Titan.Token.STANDARD_INDEX)) {
//                        tx.mutateEdgeIndex(getIndexKey(value),
//                                Lists.newArrayList(StaticArrayEntry.of(getIDIndexColumn(key, relation.getID()),
//                                        relationID2Buffer((RelationIdentifier) relation.getId()))), NO_DELETIONS);
//                    } else {
//                        addKeyValue(relation, key, value, index, tx);
//                    }
//                }
//            }
//        }
//    }
//
//    public void removeEdge(InternalRelation relation, BackendTransaction tx) throws StorageException {
//        Preconditions.checkArgument(relation instanceof TitanEdge, "Only edges can be indexed for now");
//        for (TitanType type : relation.getPropertyKeysDirect()) {
//            if (type instanceof TitanKey) {
//                TitanKey key = (TitanKey) type;
//                for (String index : key.getIndexes(Edge.class)) {
//                    Object value = relation.getPropertyDirect(key);
//                    if (index.equals(Titan.Token.STANDARD_INDEX)) {
//                        tx.mutateEdgeIndex(getIndexKey(value), NO_ADDITIONS,
//                                Lists.newArrayList(getIDIndexColumn(key, relation.getID())));
//                    } else {
//                        removeKeyValue(relation, key, index, tx);
//                    }
//                }
//            }
//        }
//    }



    /* ################################################
                Querying
    ################################################### */

    public List<Object> query(final JointIndexQuery.Subquery query, final BackendTransaction tx) {
        IndexType index = query.getIndex();
        if (index.isInternalIndex()) {
            KeySliceQuery sq = query.getInternalQuery();
            EntryList r = tx.indexQuery(sq);
            List<Object> results = new ArrayList<Object>(r.size());
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
            Preconditions.checkArgument(((InternalIndexType)index).getCardinality()!=Cardinality.SINGLE || results.size() <= 1);
            return results;
        } else {
            List<String> r = tx.indexQuery(((ExternalIndexType) index).getIndexName(), query.getExternalQuery());
            List<Object> result = new ArrayList<Object>(r.size());
            for (String id : r) result.add(string2ElementId(id));
            return result;
        }
    }

    public KeySliceQuery getQuery(final InternalIndexType index, Object[] values) {
        return new KeySliceQuery(getIndexKey(index,values), BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(1));
    }

    public IndexQuery getQuery(final ExternalIndexType index, final Condition condition, final OrderList orders) {
        Condition newCondition = ConditionUtil.literalTransformation(condition,
                new Function<Condition<TitanElement>, Condition<TitanElement>>() {
                    @Nullable
                    @Override
                    public Condition<TitanElement> apply(@Nullable Condition<TitanElement> condition) {
                        Preconditions.checkArgument(condition instanceof PredicateCondition);
                        PredicateCondition pc = (PredicateCondition) condition;
                        TitanKey key = (TitanKey) pc.getKey();
                        return new PredicateCondition<String, TitanElement>(key2Field(index,key), pc.getPredicate(), pc.getValue());
                    }
                });
        ImmutableList<IndexQuery.OrderEntry> newOrders = IndexQuery.NO_ORDER;
        if (!orders.isEmpty()) {
            ImmutableList.Builder<IndexQuery.OrderEntry> lb = ImmutableList.builder();
            for (int i = 0; i < orders.size(); i++) {
                lb.add(new IndexQuery.OrderEntry(key2Field(index,orders.getKey(i)), orders.getOrder(i), orders.getKey(i).getDataType()));
            }
            newOrders = lb.build();
        }
        return new IndexQuery(getStoreName(index.getElement()), newCondition, newOrders);
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
        ExternalIndexType index = getExternalIndex(query.getIndex(),resultType,transaction);

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
                            || (quoteTerminated && qB.charAt(pos)!='"')) ) {
                keyBuilder.append(qB.charAt(pos));
                pos++;
            }
            if (quoteTerminated) pos++;
            int endPos = pos;
            String keyname = keyBuilder.toString();
            Preconditions.checkArgument(StringUtils.isNotBlank(keyname),
                    "Found reference to empty key at position [%s]",startPos);
            String replacement;
            if (transaction.containsType(keyname)) {
                TitanKey key = transaction.getPropertyKey(keyname);
                Preconditions.checkNotNull(key);
                Preconditions.checkArgument(index.indexesKey(key),
                        "The used key [%s] is not indexed in the targeted index [%s]",key.getName(),query.getIndex());
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
        Preconditions.checkArgument(replacements>0,"Could not convert given %s index query: %s",resultType, query.getQuery());
        log.info("Converted query string with {} replacements: [{}] => [{}]",replacements,query.getQuery(),queryStr);
        RawQuery rawQuery=new RawQuery(getStoreName(resultType),queryStr,query.getParameters());
        if (query.hasLimit()) rawQuery.setLimit(query.getLimit());
        return Iterables.transform(backendTx.rawQuery(query.getIndex(), rawQuery), new Function<RawQuery.Result<String>, RawQuery.Result>() {
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

    private static ExternalIndexType getExternalIndex(String indexName, ElementCategory elementCategory, StandardTitanTx tx) {
        String indexLookupName = ExternalIndexTypeWrapper.getExternalIndexName(indexName,elementCategory);
        ExternalIndexType index = null;//retrieve actual index from transaction
        Preconditions.checkArgument(index!=null,"There is no index [%s] installed for element category [%s]",indexName,elementCategory);
        return index;
    }

    private static final boolean isStandardIndex(String index) {
        return index.equals(Titan.Token.STANDARD_INDEX);
    }

    private static final String element2String(TitanElement element) {
        if (element instanceof TitanVertex) return longID2Name(element.getID());
        else {
            RelationIdentifier rid = (RelationIdentifier) element.getId();
            return rid.toString();
        }
    }

    private static final Object string2ElementId(String str) {
        if (str.contains(RelationIdentifier.TOSTRING_DELIMITER)) return RelationIdentifier.parse(str);
        else return name2LongID(str);
    }

    private static final String key2Field(ExternalIndexType index, TitanKey key) {
        return key2Field(index.getField(key));
    }

    private static final String key2Field(ParameterIndexField field) {
        assert field!=null;
        return ParameterType.MAPPED_NAME.findParameter(field.getParameters(),longID2Name(field.getFieldKey().getID()));
    }

    private static final String longID2Name(long id) {
        Preconditions.checkArgument(id > 0);
        return LongEncoding.encode(id);
    }

    private static final long name2LongID(String name) {
        return LongEncoding.decode(name);
    }

    private static final String getStoreName(ElementCategory type) {
        return type.getName();
    }

    private static final ElementCategory getElementCategory(String store) {
        return ElementCategory.getByName(store);
    }




    private final StaticBuffer getIndexKey(InternalIndexType index, RecordEntry[] record) {
        return getIndexKey(index,IndexRecords.getValues(record));
    }

    private final StaticBuffer getIndexKey(InternalIndexType index, Object[] values) {
        DataOutput out = serializer.getDataOutput(8*DEFAULT_OBJECT_BYTELEN + 8);
        IndexField[] fields = index.getFields();
        Preconditions.checkArgument(fields.length>0 && fields.length==values.length);
        for (int i = 0; i < fields.length; i++) {
            IndexField f = fields[i];
            Object value = values[i];
            Preconditions.checkNotNull(value);
            if (AttributeUtil.hasGenericDataType(f.getFieldKey())) out.writeClassAndObject(value);
            else out.writeObjectNotNull(value);
        }
        VariableLong.writePositive(out, index.getID());
        return out.getStaticBuffer();
    }

    private final Entry getIndexEntry(InternalIndexType index, RecordEntry[] record, TitanElement element) {
        DataOutput out = serializer.getDataOutput(1+8+8*record.length+4*8);
        out.putByte(FIRST_INDEX_COLUMN_BYTE);
        if (index.getCardinality()!=Cardinality.SINGLE) {
            VariableLong.writePositive(out,element.getID());
            if (index.getCardinality()!=Cardinality.SET) {
                for (RecordEntry re : record) {
                    VariableLong.writePositive(out,re.relationId);
                }
            }
        }
        int valuePosition=out.getPosition();
        if (element instanceof TitanVertex) {
            VariableLong.writePositive(out,element.getID());
        } else {
            assert element instanceof TitanRelation;
            RelationIdentifier rid = (RelationIdentifier)element.getId();
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

//    private static final StaticBuffer relationID2Buffer(RelationIdentifier rid) {
//        long[] longs = rid.getLongRepresentation();
//        Preconditions.checkArgument(longs.length == 3 || longs.length == 4);
//        WriteBuffer buffer = new WriteByteBuffer(longs.length* StaticArrayBuffer.LONG_LEN);
//        for (int i = 0; i < longs.length; i++) VariableLong.writePositive(buffer, longs[i]);
//        return buffer.getStaticBuffer();
//    }

//    private final StaticBuffer getIndexKey(Object att) {
//        DataOutput out = serializer.getDataOutput(DEFAULT_VALUE_CAPACITY);
//        out.writeObjectNotNull(att);
//        return out.getStaticBuffer();
//    }

//    private static final StaticBuffer getIndexValue(TitanProperty prop) {
//        return VariableLong.positiveBuffer(new long[]{prop.getVertex().getID(), prop.getID()});
//    }
//
//    private static final StaticBuffer getIndexColumn(TitanKey key, TitanProperty prop) {
//        if (key.isUnique(Direction.IN)) {
//            return getUniqueIndexColumn(key);
//        } else if (key.isUnique(Direction.OUT)) {
//            return getIDIndexColumn(key, prop.getVertex().getID());
//        } else {
//            return getIDIndexColumn(key, prop.getID());
//        }
//    }
//
//    private static final StaticBuffer getUniqueIndexColumn(TitanKey type) {
//        return VariableLong.positiveBuffer(type.getID());
//    }
//
//    private static final StaticBuffer getIDIndexColumn(TitanKey type, long propertyID) {
//        return VariableLong.positiveBuffer(new long[]{type.getID(), propertyID});
//    }

}
