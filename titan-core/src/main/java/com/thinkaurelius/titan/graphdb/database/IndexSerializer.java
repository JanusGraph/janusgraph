package com.thinkaurelius.titan.graphdb.database;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.indexing.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry;
import com.thinkaurelius.titan.diskstorage.util.WriteByteBuffer;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.internal.ElementType;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.internal.OrderList;
import com.thinkaurelius.titan.graphdb.query.IndexQueryBuilder;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.thinkaurelius.titan.graphdb.query.condition.*;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.IndexDefinition;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanKeyVertex;
import com.thinkaurelius.titan.util.encoding.LongEncoding;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

import static com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore.NO_ADDITIONS;
import static com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore.NO_DELETIONS;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IndexSerializer {

    private static final Logger log = LoggerFactory.getLogger(IndexSerializer.class);

    private static final int DEFAULT_VALUE_CAPACITY = 40;

    private final Serializer serializer;
    private final Map<String, ? extends IndexInformation> indexes;

    public IndexSerializer(Serializer serializer, Map<String, ? extends IndexInformation> indexes) {
        this.serializer = serializer;
        this.indexes = indexes;
    }

//    public IndexInformation getIndexInformation(String indexName) {
//        IndexInformation indexinfo = indexes.get(indexName);
//        Preconditions.checkArgument(indexinfo != null, "Index is unknown or not configured: %s", indexName);
//        return indexinfo;
//    }

    public boolean supports(final String indexName, final Class<?> dataType, final Parameter[] parameters) {
        IndexInformation indexinfo = indexes.get(indexName);
        Preconditions.checkArgument(indexinfo != null, "Index is unknown or not configured: %s", indexName);
        return indexinfo.supports(new StandardKeyInformation(dataType,parameters));
    }

    public boolean supports(final String indexName, final ElementType result, final TitanKey key, final TitanPredicate predicate) {
        IndexInformation indexinfo = indexes.get(indexName);
        Preconditions.checkArgument(indexinfo != null, "Index is unknown or not configured: %s", indexName);
        return indexinfo.supports(getKeyInformation(key,result.getElementType(),indexName),predicate);
    }

    private static StandardKeyInformation getKeyInformation(final TitanKey key, final Class<? extends Element> type, final String index) {
        return new StandardKeyInformation(key.getDataType(),
                (key instanceof TitanKeyVertex)?
                        ((TitanKeyVertex)key).getIndex(index,type).getParameters()
                        :new Parameter[0]);
    }

    public IndexInfoRetriever getIndexInforRetriever() {
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

                @Override
                public KeyInformation get(String store, String key) {
                    Preconditions.checkState(transaction!=null,"Retriever has not been initialized");
                    long keyid = string2KeyId(key);
                    TitanKey titanKey = (TitanKey)transaction.getExistingType(keyid);
                    ElementType elementType = getElementType(store);
                    return getKeyInformation(titanKey,elementType.getElementType(),index);
                }

                @Override
                public KeyInformation.StoreRetriever get(final String store) {
                    final KeyInformation.IndexRetriever retriever = this;
                    return new KeyInformation.StoreRetriever() {
                        @Override
                        public KeyInformation get(String key) {
                            return retriever.get(store,key);
                        }
                    };
                }
            };
        }
    }

    /* ################################################
               Index Updates
    ################################################### */

    public void newPropertyKey(TitanKey key, BackendTransaction tx) throws StorageException {
        for (String index : key.getIndexes(Vertex.class)) {
            if (!index.equals(Titan.Token.STANDARD_INDEX))
                tx.getIndexTransactionHandle(index).register(ElementType.VERTEX.getName(), key2String(key), getKeyInformation(key,Vertex.class,index));
        }
        for (String index : key.getIndexes(Edge.class)) {
            if (!index.equals(Titan.Token.STANDARD_INDEX))
                tx.getIndexTransactionHandle(index).register(ElementType.EDGE.getName(), key2String(key), getKeyInformation(key, Edge.class, index));
        }
    }

    public void addProperty(TitanProperty prop, BackendTransaction tx) throws StorageException {
        TitanKey key = prop.getPropertyKey();
        for (String index : key.getIndexes(Vertex.class)) {
            if (index.equals(Titan.Token.STANDARD_INDEX)) {
                tx.mutateVertexIndex(getIndexKey(prop.getValue()),
                        Lists.newArrayList(StaticBufferEntry.of(getIndexColumn(key, prop), getIndexValue(prop))), NO_DELETIONS);
            } else {
                addKeyValue(prop.getVertex(), key, prop.getValue(), index, tx);
            }
        }
    }

    public void removeProperty(TitanProperty prop, BackendTransaction tx) throws StorageException {
        TitanKey key = prop.getPropertyKey();
        for (String index : key.getIndexes(Vertex.class)) {
            if (index.equals(Titan.Token.STANDARD_INDEX)) {
                tx.mutateVertexIndex(getIndexKey(prop.getValue()), NO_ADDITIONS,
                        Lists.newArrayList(getIndexColumn(key, prop)));
            } else {
                removeKeyValue(prop.getVertex(), key, index, tx);
            }
        }
    }

    public void lockKeyedProperty(TitanProperty prop, BackendTransaction tx) throws StorageException {
        TitanKey key = prop.getPropertyKey();
        if (key.isUnique(Direction.IN) && ((InternalType) key).uniqueLock(Direction.IN)) {
            Preconditions.checkArgument(key.hasIndex(Titan.Token.STANDARD_INDEX, Vertex.class), "Standard Index needs to be created for property to be declared unique [%s]", key.getName());
            Preconditions.checkArgument(prop.isNew() || prop.isRemoved());
            tx.acquireVertexIndexLock(getIndexKey(prop.getValue()), getIndexColumn(key, prop), prop.isNew() ? null : getIndexValue(prop));
        }
    }

    public void addEdge(InternalRelation relation, BackendTransaction tx) throws StorageException {
        Preconditions.checkArgument(relation instanceof TitanEdge, "Only edges can be indexed for now");
        for (TitanType type : relation.getPropertyKeysDirect()) {
            if (type instanceof TitanKey) {
                TitanKey key = (TitanKey) type;
                for (String index : key.getIndexes(Edge.class)) {
                    Object value = relation.getPropertyDirect(key);
                    if (index.equals(Titan.Token.STANDARD_INDEX)) {
                        tx.mutateEdgeIndex(getIndexKey(value),
                                Lists.newArrayList(StaticBufferEntry.of(getIDIndexColumn(key, relation.getID()),
                                        relationID2ByteBuffer((RelationIdentifier) relation.getId()))), NO_DELETIONS);
                    } else {
                        addKeyValue(relation, key, value, index, tx);
                    }
                }
            }
        }
    }

    public void removeEdge(InternalRelation relation, BackendTransaction tx) throws StorageException {
        Preconditions.checkArgument(relation instanceof TitanEdge, "Only edges can be indexed for now");
        for (TitanType type : relation.getPropertyKeysDirect()) {
            if (type instanceof TitanKey) {
                TitanKey key = (TitanKey) type;
                for (String index : key.getIndexes(Edge.class)) {
                    Object value = relation.getPropertyDirect(key);
                    if (index.equals(Titan.Token.STANDARD_INDEX)) {
                        tx.mutateEdgeIndex(getIndexKey(value), NO_ADDITIONS,
                                Lists.newArrayList(getIDIndexColumn(key, relation.getID())));
                    } else {
                        removeKeyValue(relation, key, index, tx);
                    }
                }
            }
        }
    }

    private void addKeyValue(TitanElement element, TitanKey key, Object value, String index, BackendTransaction tx) throws StorageException {
        Preconditions.checkArgument(key.isUnique(Direction.OUT), "Only out-unique properties are supported by index [%s]", index);
        tx.getIndexTransactionHandle(index).add(getStoreName(element), element2String(element), key2String(key), value, element.isNew());
    }

    private void removeKeyValue(TitanElement element, TitanKey key, String index, BackendTransaction tx) {
        Preconditions.checkArgument(key.isUnique(Direction.OUT), "Only out-unique properties are supported by index [%s]", index);
        tx.getIndexTransactionHandle(index).delete(getStoreName(element), element2String(element), key2String(key), element.isRemoved());
    }

    /* ################################################
                Querying
    ################################################### */

    public List<Object> query(final String indexName, final IndexQuery query, final BackendTransaction tx) {
        Preconditions.checkArgument(indexes.containsKey(indexName), "Index unknown or unconfigured: %s", indexName);
        if (isStandardIndex(indexName)) {
            Preconditions.checkArgument(query.getOrder().isEmpty(), "Standard index does not support ordering");
            List<Object> results = null;
            final ElementType resultType = getElementType(query.getStore());
            final Condition<?> condition = query.getCondition();

            if (condition instanceof And) {
                List<QueryUtil.IndexCall<Object>> retrievals = new ArrayList<QueryUtil.IndexCall<Object>>(condition.numChildren());
                for (final Condition<?> subcond : ((And<?>) condition).getChildren()) {
                    retrievals.add(new QueryUtil.IndexCall<Object>() {
                        @Override
                        public Collection<Object> call(final int limit) {
                            Preconditions.checkArgument(limit >= 0);
                            List<Object> r = null;
                            if (subcond instanceof Or) { //Concatenate results until we have enough for limit
                                r = new ArrayList<Object>(limit);
                                for (Condition nested : subcond.getChildren()) {
                                    Preconditions.checkArgument(nested instanceof PredicateCondition, "Invalid query (not in QNF): %s", condition);
                                    r.addAll(processSingleCondition(resultType, (PredicateCondition) nested, limit, tx));
                                    if (r.size() >= limit) break;
                                }
                            } else if (subcond instanceof PredicateCondition) {
                                r = processSingleCondition(resultType, (PredicateCondition) subcond, limit, tx);
                            } else
                                throw new IllegalArgumentException("Invalid query provided (original not in QNF):" + subcond);
                            return r;
                        }
                    });
                }
                results = QueryUtil.processIntersectingRetrievals(retrievals, query.getLimit());
            } else {
                Preconditions.checkArgument(false, "Invalid query (not in QNF): %s", condition);
            }
            return results;
        } else {
            List<String> r = tx.indexQuery(indexName, query);
            List<Object> result = new ArrayList<Object>(r.size());
            for (String id : r) result.add(string2ElementId(id));
            return result;
        }
    }

    private List<Object> processSingleCondition(ElementType resultType, PredicateCondition pc, final int limit, BackendTransaction tx) {
        Preconditions.checkArgument(resultType == ElementType.EDGE || resultType == ElementType.VERTEX);
        Preconditions.checkArgument(pc.getPredicate() == Cmp.EQUAL, "Only equality index retrievals are supported on standard index");
        Preconditions.checkNotNull(pc.getValue());
        Preconditions.checkArgument(limit >= 0);
        TitanKey key = (TitanKey) pc.getKey();
        Preconditions.checkArgument(key.hasIndex(Titan.Token.STANDARD_INDEX, resultType.getElementType()),
                "Cannot retrieve for given property key - it does not have an index [%s]", key.getName());
        Object value = pc.getValue();
        StaticBuffer column = getUniqueIndexColumn(key);
        KeySliceQuery sq = new KeySliceQuery(getIndexKey(value), column, SliceQuery.pointRange(column), ((InternalType) key).isStatic(Direction.IN)).setLimit(limit);
        List<Entry> r;
        if (resultType == ElementType.VERTEX) {
            r = tx.vertexIndexQuery(sq);
        } else {
            r = tx.edgeIndexQuery(sq);
        }
        List<Object> results = new ArrayList<Object>(r.size());
        for (Entry entry : r) {
            ReadBuffer entryValue = entry.getReadValue();
            if (resultType == ElementType.VERTEX) {
                results.add(VariableLong.readPositive(entryValue));
            } else {
                results.add(bytebuffer2RelationId(entryValue));
            }
        }
        Preconditions.checkArgument(!(resultType == ElementType.VERTEX && key.isUnique(Direction.IN)) || results.size() <= 1);
        return results;
    }

    public IndexQuery getQuery(String index, final ElementType resultType, final Condition condition, final OrderList orders) {
        if (isStandardIndex(index)) {
            Preconditions.checkArgument(orders.isEmpty());
            return new IndexQuery(getStoreName(resultType), condition, IndexQuery.NO_ORDER);
        } else {
            Condition newCondition = ConditionUtil.literalTransformation(condition,
                    new Function<Condition<TitanElement>, Condition<TitanElement>>() {
                        @Nullable
                        @Override
                        public Condition<TitanElement> apply(@Nullable Condition<TitanElement> condition) {
                            Preconditions.checkArgument(condition instanceof PredicateCondition);
                            PredicateCondition pc = (PredicateCondition) condition;
                            TitanKey key = (TitanKey) pc.getKey();
                            return new PredicateCondition<String, TitanElement>(key2String(key), pc.getPredicate(), pc.getValue());
                        }
                    });
            ImmutableList<IndexQuery.OrderEntry> newOrders = IndexQuery.NO_ORDER;
            if (!orders.isEmpty()) {
                ImmutableList.Builder<IndexQuery.OrderEntry> lb = ImmutableList.builder();
                for (int i = 0; i < orders.size(); i++) {
                    lb.add(new IndexQuery.OrderEntry(key2String(orders.getKey(i)), orders.getOrder(i), orders.getKey(i).getDataType()));
                }
                newOrders = lb.build();
            }
            return new IndexQuery(getStoreName(resultType), newCondition, newOrders);
        }
    }

    public Iterable<RawQuery.Result> executeQuery(IndexQueryBuilder query, final ElementType resultType,
                                                  final BackendTransaction backendTx, final StandardTitanTx transaction) {
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
            Preconditions.checkArgument(transaction.containsType(keyname),
                    "Found reference to non-existant property key in query at position [%s]: %s",startPos,keyname);
            TitanKey key = transaction.getPropertyKey(keyname);
            Preconditions.checkArgument(key.hasIndex(query.getIndex(),resultType.getElementType()),
                    "The used key [%s] is not indexed in the targeted index [%s]",key.getName(),query.getIndex());
            String replacement = key2String(key);
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

    private static final boolean isStandardIndex(String index) {
        return index.equals(Titan.Token.STANDARD_INDEX);
    }

    private static final StaticBuffer relationID2ByteBuffer(RelationIdentifier rid) {
        long[] longs = rid.getLongRepresentation();
        Preconditions.checkArgument(longs.length == 3);
        WriteBuffer buffer = new WriteByteBuffer(24);
        for (int i = 0; i < 3; i++) VariableLong.writePositive(buffer, longs[i]);
        return buffer.getStaticBuffer();
    }

    private static final RelationIdentifier bytebuffer2RelationId(ReadBuffer b) {
        long[] relationId = new long[3];
        for (int i = 0; i < 3; i++) relationId[i] = VariableLong.readPositive(b);
        return RelationIdentifier.get(relationId);
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

    private static final String key2String(TitanKey key) {
        return longID2Name(key.getID());
    }

    private static final long string2KeyId(String key) {
        return name2LongID(key);
    }

    private static final String longID2Name(long id) {
        Preconditions.checkArgument(id > 0);
        return LongEncoding.encode(id);
    }

    private static final long name2LongID(String name) {
        return LongEncoding.decode(name);
    }

    private static final String getStoreName(Element element) {
        if (element instanceof TitanVertex) return getStoreName(ElementType.VERTEX);
        else if (element instanceof TitanEdge) return getStoreName(ElementType.EDGE);
        else throw new IllegalArgumentException("Invalid class: " + element.getClass());
    }

    private static final String getStoreName(ElementType type) {
        return type.getName();
    }

    private static final ElementType getElementType(String store) {
        return ElementType.getByName(store);
    }

    private final StaticBuffer getIndexKey(Object att) {
        DataOutput out = serializer.getDataOutput(DEFAULT_VALUE_CAPACITY, true);
        out.writeObjectNotNull(att);
        return out.getStaticBuffer();
    }

    private static final StaticBuffer getIndexValue(TitanProperty prop) {
        return VariableLong.positiveByteBuffer(new long[]{prop.getVertex().getID(), prop.getID()});
    }

    private static final StaticBuffer getIndexColumn(TitanKey key, TitanProperty prop) {
        if (key.isUnique(Direction.IN)) {
            return getUniqueIndexColumn(key);
        } else if (key.isUnique(Direction.OUT)) {
            return getIDIndexColumn(key, prop.getVertex().getID());
        } else {
            return getIDIndexColumn(key, prop.getID());
        }
    }

    private static final StaticBuffer getUniqueIndexColumn(TitanKey type) {
        return VariableLong.positiveByteBuffer(type.getID());
    }

    private static final StaticBuffer getIDIndexColumn(TitanKey type, long propertyID) {
        return VariableLong.positiveByteBuffer(new long[]{type.getID(), propertyID});
    }

}
