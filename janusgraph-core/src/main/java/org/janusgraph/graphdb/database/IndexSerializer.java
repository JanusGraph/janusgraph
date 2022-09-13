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
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.janusgraph.core.JanusGraphElement;
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
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexFeatures;
import org.janusgraph.diskstorage.indexing.IndexInformation;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.diskstorage.indexing.StandardKeyInformation;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.HashingUtil;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.graphdb.database.index.IndexInfoRetriever;
import org.janusgraph.graphdb.database.index.IndexMutationType;
import org.janusgraph.graphdb.database.index.IndexRecords;
import org.janusgraph.graphdb.database.index.IndexUpdate;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.database.util.IndexRecordUtil;
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
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.tinkerpop.optimize.step.Aggregation;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.ParameterIndexField;
import org.janusgraph.graphdb.types.ParameterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_NAME_MAPPING;
import static org.janusgraph.graphdb.database.util.IndexRecordUtil.bytebuffer2RelationId;
import static org.janusgraph.graphdb.database.util.IndexRecordUtil.getCompositeIndexUpdate;
import static org.janusgraph.graphdb.database.util.IndexRecordUtil.element2String;
import static org.janusgraph.graphdb.database.util.IndexRecordUtil.getIndexTTL;
import static org.janusgraph.graphdb.database.util.IndexRecordUtil.getKeyInformation;
import static org.janusgraph.graphdb.database.util.IndexRecordUtil.getKeysOfRecords;
import static org.janusgraph.graphdb.database.util.IndexRecordUtil.getMixedIndexUpdate;
import static org.janusgraph.graphdb.database.util.IndexRecordUtil.getUpdateType;
import static org.janusgraph.graphdb.database.util.IndexRecordUtil.indexAppliesTo;
import static org.janusgraph.graphdb.database.util.IndexRecordUtil.indexMatch;
import static org.janusgraph.graphdb.database.util.IndexRecordUtil.indexMatches;
import static org.janusgraph.graphdb.database.util.IndexRecordUtil.key2Field;
import static org.janusgraph.graphdb.database.util.IndexRecordUtil.keyID2Name;
import static org.janusgraph.graphdb.database.util.IndexRecordUtil.string2ElementId;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IndexSerializer {

    private static final Logger log = LoggerFactory.getLogger(IndexSerializer.class);

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

    public IndexInfoRetriever getIndexInfoRetriever(StandardJanusGraphTx tx) {
        return new IndexInfoRetriever(tx);
    }

    /* ################################################
               Index Updates
    ################################################### */

    public Collection<IndexUpdate> getIndexUpdates(InternalRelation relation) {
        assert relation.isNew() || relation.isRemoved();
        final Set<IndexUpdate> updates = new HashSet<>();
        final IndexMutationType updateType = getUpdateType(relation);
        final int ttl = updateType==IndexMutationType.ADD?StandardJanusGraph.getTTL(relation):0;
        for (final PropertyKey type : relation.getPropertyKeysDirect()) {
            if (type == null) continue;
            for (final IndexType index : ((InternalRelationType) type).getKeyIndexes()) {
                if (!indexAppliesTo(index,relation)) continue;
                IndexUpdate update;
                if (index instanceof CompositeIndexType) {
                    final CompositeIndexType iIndex= (CompositeIndexType) index;
                    final IndexRecordEntry[] record = indexMatch(relation, iIndex);
                    if (record==null) continue;
                    update = getCompositeIndexUpdate(iIndex, updateType, record, relation, serializer, hashKeys, hashLength);
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

    public Collection<IndexUpdate> getIndexUpdates(InternalVertex vertex, Collection<InternalRelation> updatedProperties) {
        if (updatedProperties.isEmpty()) return Collections.emptyList();
        final Set<IndexUpdate> updates = new HashSet<>();

        for (final InternalRelation rel : updatedProperties) {
            assert rel.isProperty();
            final JanusGraphVertexProperty p = (JanusGraphVertexProperty)rel;
            assert rel.isNew() || rel.isRemoved(); assert rel.getVertex(0).equals(vertex);
            final IndexMutationType updateType = getUpdateType(rel);
            for (final IndexType index : ((InternalRelationType)p.propertyKey()).getKeyIndexes()) {
                if (!indexAppliesTo(index,vertex)) continue;
                if (index.isCompositeIndex()) { //Gather composite indexes
                    final CompositeIndexType cIndex = (CompositeIndexType)index;
                    final IndexRecords updateRecords = indexMatches(vertex,cIndex,updateType==IndexMutationType.DELETE,p.propertyKey(),new IndexRecordEntry(p));
                    for (final IndexRecordEntry[] record : updateRecords) {
                        final IndexUpdate update = getCompositeIndexUpdate(cIndex, updateType, record, vertex, serializer, hashKeys, hashLength);
                        final int ttl = getIndexTTL(vertex,getKeysOfRecords(record));
                        if (ttl>0 && updateType== IndexMutationType.ADD) update.setTTL(ttl);
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
                    if (ttl>0 && updateType== IndexMutationType.ADD) update.setTTL(ttl);
                    updates.add(update);
                }
            }
        }
        return updates;
    }

    public boolean reindexElement(JanusGraphElement element, MixedIndexType index, Map<String,Map<String,List<IndexEntry>>> documentsPerStore) {
        if (!indexAppliesTo(index, element))
            return false;
        final List<IndexEntry> entries = new ArrayList<>();
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
        return documentsPerStore.computeIfAbsent(index.getStoreName(), k -> new HashMap<>());
    }

    public void removeElement(Object elementId, MixedIndexType index, Map<String,Map<String,List<IndexEntry>>> documentsPerStore) {
        Preconditions.checkArgument((index.getElement()==ElementCategory.VERTEX && elementId instanceof Long) ||
            (index.getElement().isRelation() && elementId instanceof RelationIdentifier),"Invalid element id [%s] provided for index: %s",elementId,index);
        getDocuments(documentsPerStore,index).put(element2String(elementId),new ArrayList<>());
    }

    public Set<IndexUpdate<StaticBuffer,Entry>> reindexElement(JanusGraphElement element, CompositeIndexType index) {
        final Set<IndexUpdate<StaticBuffer,Entry>> indexEntries = new HashSet<>();
        if (!indexAppliesTo(index,element)) {
            return indexEntries;
        }
        Iterable<IndexRecordEntry[]> records;
        if (element instanceof JanusGraphVertex) {
            records = indexMatches((JanusGraphVertex)element,index);
        } else {
            assert element instanceof JanusGraphRelation;
            final IndexRecordEntry[] record = indexMatch((JanusGraphRelation)element,index);
            records = (record == null) ? Collections.emptyList() : Collections.singletonList(record);
        }
        for (final IndexRecordEntry[] record : records) {
            indexEntries.add(getCompositeIndexUpdate(index, IndexMutationType.ADD, record, element, serializer, hashKeys, hashLength));
        }
        return indexEntries;
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
            return tx.indexQuery(index.getBackingIndexName(), query.getMixedQuery()).map(IndexRecordUtil::string2ElementId);
        }
    }

    public Number queryAggregation(final JointIndexQuery.Subquery query, final BackendTransaction tx, final Aggregation aggregation) {
        final IndexType index = query.getIndex();
        assert index.isMixedIndex();
        return tx.indexQueryAggregation(index.getBackingIndexName(), query.getMixedQuery(), aggregation);
    }

    public MultiKeySliceQuery getQuery(final CompositeIndexType index, List<Object[]> values) {
        final List<KeySliceQuery> ksqs = new ArrayList<>(values.size());
        for (final Object[] value : values) {
            ksqs.add(new KeySliceQuery(IndexRecordUtil.getIndexKey(index, value, serializer, hashKeys, hashLength),
                BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(1)));
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
        final MixedIndexType index = IndexRecordUtil.getMixedIndex(query.getIndex(), transaction);
        final String queryStr = createQueryString(query, resultType, transaction, index);
        ImmutableList<IndexQuery.OrderEntry> orders = getOrders(query, resultType, transaction, index);
        final RawQuery rawQuery = new RawQuery(index.getStoreName(),queryStr,orders,query.getParameters());
        if (query.hasLimit()) rawQuery.setLimit(query.getLimit());
        rawQuery.setOffset(query.getOffset());
        return backendTx.rawQuery(index.getBackingIndexName(), rawQuery).map(result ->  new RawQuery.Result(string2ElementId(result.getResult()), result.getScore()));
    }

    public Long executeTotals(IndexQueryBuilder query, final ElementCategory resultType,
                              final BackendTransaction backendTx, final StandardJanusGraphTx transaction) {
        final MixedIndexType index = IndexRecordUtil.getMixedIndex(query.getIndex(), transaction);
        final String queryStr = createQueryString(query, resultType, transaction, index);
        final RawQuery rawQuery = new RawQuery(index.getStoreName(),queryStr,query.getParameters());
        if (query.hasLimit()) rawQuery.setLimit(query.getLimit());
        rawQuery.setOffset(query.getOffset());
        return backendTx.totals(index.getBackingIndexName(), rawQuery);
    }

    public long getIndexIdFromKey(StaticBuffer key) {
        return IndexRecordUtil.getIndexIdFromKey(key, hashKeys, hashLength);
    }

    public boolean isHashKeys() {
        return hashKeys;
    }

    public HashingUtil.HashLength getHashLength() {
        return hashLength;
    }
}
