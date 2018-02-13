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

package org.janusgraph.diskstorage.lucene;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Shape;

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.core.attribute.*;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.indexing.*;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.serialize.AttributeUtil;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.query.condition.*;
import org.janusgraph.graphdb.types.ParameterType;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.vector.PointVectorStrategy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class LuceneIndex implements IndexProvider {
    private static final Logger log = LoggerFactory.getLogger(LuceneIndex.class);

    private static final String DOCID = "_____elementid";
    private static final String GEOID = "_____geo";

    private static final IndexFeatures LUCENE_FEATURES = new IndexFeatures.Builder()
        .supportedStringMappings(Mapping.TEXT, Mapping.STRING)
        .supportsCardinality(Cardinality.SINGLE)
        .supportsCustomAnalyzer()
        .supportsCardinality(Cardinality.LIST)
        .supportsCardinality(Cardinality.SET)
        .supportsNanoseconds()
        .supportsGeoContains()
        .build();

    /**
     * Default tree levels used when creating the prefix tree.
     */
    public static final int DEFAULT_GEO_MAX_LEVELS = 20;

    /**
     * Default measure of shape precision used when creating the prefix tree.
     */
    public static final double DEFAULT_GEO_DIST_ERROR_PCT = 0.025;

    private static final Map<Geo, SpatialOperation> SPATIAL_PREDICATES = spatialPredicates();

    private final Map<String, IndexWriter> writers = new HashMap<>(4);
    private final ReentrantLock writerLock = new ReentrantLock();

    private final Map<String, SpatialStrategy> spatial = new ConcurrentHashMap<>(12);
    private final SpatialContext ctx = Geoshape.getSpatialContext();

    private final String basePath;

    /**
     * lazy cache for the delegating analyzers used for writting or querrying for each store
     */
    private final Map<String, LuceneCustomAnalyzer> delegatingAnalyzers = new HashMap<>();

    public LuceneIndex(Configuration config) {
        final String dir = config.get(GraphDatabaseConfiguration.INDEX_DIRECTORY);
        final File directory = new File(dir);
        if ((!directory.exists() && !directory.mkdirs()) || !directory.isDirectory() || !directory.canWrite()) {
            throw new IllegalArgumentException("Cannot access or write to directory: " + dir);
        }
        basePath = directory.getAbsolutePath();
        log.debug("Configured Lucene to use base directory [{}]", basePath);
    }

    private Directory getStoreDirectory(String store) throws BackendException {
        Preconditions.checkArgument(StringUtils.isAlphanumeric(store), "Invalid store name: %s", store);
        final String dir = basePath + File.separator + store;
        try {
            final File path = new File(dir);
            if ((!path.exists() && !path.mkdirs()) || !path.isDirectory() || !path.canWrite()) {
                throw new PermanentBackendException("Cannot access or write to directory: " + dir);
            }
            log.debug("Opening store directory [{}]", path);
            return FSDirectory.open(path.toPath());
        } catch (final IOException e) {
            throw new PermanentBackendException("Could not open directory: " + dir, e);
        }
    }

    private IndexWriter getWriter(String store, KeyInformation.IndexRetriever informations) throws BackendException {
        Preconditions.checkArgument(writerLock.isHeldByCurrentThread());
        IndexWriter writer = writers.get(store);
        if (writer == null) {
            final LuceneCustomAnalyzer analyzer = delegatingAnalyzerFor(store, informations);
            final IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            try {
                writer = new IndexWriter(getStoreDirectory(store), iwc);
                writers.put(store, writer);
            } catch (final IOException e) {
                throw new PermanentBackendException("Could not create writer", e);
            }
        }
        return writer;
    }

    private SpatialStrategy getSpatialStrategy(String key, KeyInformation ki) {
        SpatialStrategy strategy = spatial.get(key);
        final Mapping mapping = Mapping.getMapping(ki);
        final int maxLevels = ParameterType.INDEX_GEO_MAX_LEVELS.findParameter(ki.getParameters(),
            DEFAULT_GEO_MAX_LEVELS);
        final double distErrorPct = ParameterType.INDEX_GEO_DIST_ERROR_PCT.findParameter(ki.getParameters(),
            DEFAULT_GEO_DIST_ERROR_PCT);
        if (strategy == null) {
            synchronized (spatial) {
                if (!spatial.containsKey(key)) {
//                    SpatialPrefixTree grid = new GeohashPrefixTree(ctx, GEO_MAX_LEVELS);
//                    strategy = new RecursivePrefixTreeStrategy(grid, key);
                    if (mapping == Mapping.DEFAULT) {
                        strategy = PointVectorStrategy.newInstance(ctx, key);
                    } else {
                        final SpatialPrefixTree grid = new QuadPrefixTree(ctx, maxLevels);
                        strategy = new RecursivePrefixTreeStrategy(grid, key);
                        ((PrefixTreeStrategy) strategy).setDistErrPct(distErrorPct);
                    }
                    spatial.put(key, strategy);
                } else return spatial.get(key);
            }
        }
        return strategy;
    }

    private static Map<Geo, SpatialOperation> spatialPredicates() {
        return Collections.unmodifiableMap(Stream.of(
            new SimpleEntry<>(Geo.WITHIN, SpatialOperation.IsWithin),
            new SimpleEntry<>(Geo.CONTAINS, SpatialOperation.Contains),
            new SimpleEntry<>(Geo.INTERSECT, SpatialOperation.Intersects),
            new SimpleEntry<>(Geo.DISJOINT, SpatialOperation.IsDisjointTo))
            .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue)));
    }

    @Override
    public void register(String store, String key, KeyInformation information, BaseTransaction tx) throws BackendException {
        final Class<?> dataType = information.getDataType();
        final Mapping map = Mapping.getMapping(information);
        Preconditions.checkArgument(map == Mapping.DEFAULT || AttributeUtil.isString(dataType) ||
                (map == Mapping.PREFIX_TREE && AttributeUtil.isGeo(dataType)),
            "Specified illegal mapping [%s] for data type [%s]", map, dataType);
    }

    @Override
    public void mutate(Map<String, Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {
        final Transaction ltx = (Transaction) tx;
        writerLock.lock();
        try {
            for (final Map.Entry<String, Map<String, IndexMutation>> stores : mutations.entrySet()) {
                mutateStores(stores, information);
            }
            ltx.postCommit();
        } catch (final IOException e) {
            throw new TemporaryBackendException("Could not update Lucene index", e);
        } finally {
            writerLock.unlock();
        }
    }

    private void mutateStores(Map.Entry<String, Map<String, IndexMutation>> stores, KeyInformation.IndexRetriever informations) throws IOException, BackendException {
        final String storename = stores.getKey();
        final IndexWriter writer = getWriter(storename, informations);
        try (final IndexReader reader = DirectoryReader.open(writer, true, true)) {
            final IndexSearcher searcher = new IndexSearcher(reader);
            for (final Map.Entry<String, IndexMutation> entry : stores.getValue().entrySet()) {
                final String docid = entry.getKey();
                final IndexMutation mutation = entry.getValue();
                updateDocumentForIndexing(writer, searcher, storename, informations, docid, mutation.isDeleted(), mutation.getDeletions(), mutation.getAdditions());
            }
            writer.commit();
        }
    }

    private Document retrieveOrCreateDocument(String docID, IndexSearcher searcher) throws IOException {
        final Document doc;
        // we only need 2 results for what we do
        final TopDocs hits = searcher.search(new TermQuery(new Term(DOCID, docID)), 2);

        if (hits.scoreDocs.length > 1)
            throw new IllegalArgumentException("More than one document found for document id: " + docID);

        if (hits.scoreDocs.length == 0) {
            if (log.isTraceEnabled())
                log.trace("Creating new document for [{}]", docID);

            doc = new Document();
            doc.add(new StringField(DOCID, docID, Field.Store.YES));
        } else {
            if (log.isTraceEnabled())
                log.trace("Updating existing document for [{}]", docID);

            int docId = hits.scoreDocs[0].doc;
            //retrieve the old document
            doc = searcher.doc(docId);
        }
        return doc;
    }

    private FieldPair fieldPair(IndexableField indexableField) {
        return new FieldPair(indexableField.name(), indexableField.stringValue());
    }

    private void updateDocumentForIndexing(IndexWriter writer, IndexSearcher searcher, String storename, KeyInformation.IndexRetriever informations, String docid, boolean isDeleted, List<IndexEntry> deletions, List<IndexEntry> additions) throws IOException {
        if (isDeleted) {
            if (log.isTraceEnabled())
                log.trace("Deleted entire document [{}]", docid);

            writer.deleteDocuments(new Term(DOCID, docid));
            return;
        }

        // before mutation, restore the old document with it's stored fields
        final Document doc = retrieveOrCreateDocument(docid, searcher);
        Preconditions.checkNotNull(doc);

        // lazy mapping lookup, needed for string values
        final Function<String, Mapping> mappingFactory = (f) -> Mapping.getMapping(storename, f, informations);

        final Set<String> singleCardinalityDeletions = new HashSet<>();
        final Map<String, Object> singleCardinalityAdditions = new HashMap<>();
        final Map<String, Set<FieldPair>> setAndListCardinalityDeletions = new HashMap<>();
        final Map<String, List<IndexableField>> listAndSetCardinalityGroupedAdditions = new HashMap<>();
        for (IndexEntry indexEntry : additions) {
            Cardinality cardinality = informations.get(storename, indexEntry.field).getCardinality();
            if (cardinality == Cardinality.SINGLE) {
                // we will remove this field from the old document
                singleCardinalityDeletions.add(indexEntry.field);
                // and later re-add the last such field to the new document
                singleCardinalityAdditions.put(indexEntry.field, indexEntry.value);
            } else if (cardinality == Cardinality.LIST || cardinality == Cardinality.SET) {
                // we collect a list of the values that should be added
                if (!listAndSetCardinalityGroupedAdditions.containsKey(indexEntry.field)) {
                    listAndSetCardinalityGroupedAdditions.put(indexEntry.field, new ArrayList<>());
                }
                listAndSetCardinalityGroupedAdditions.get(indexEntry.field).add(storedField(indexEntry.field, indexEntry.value, false, mappingFactory));
            } else throw new IllegalArgumentException("cardinality not supported: " + cardinality);
        }

        for (IndexEntry indexEntry : deletions) {
            Cardinality cardinality = informations.get(storename, indexEntry.field).getCardinality();
            if (cardinality == Cardinality.SINGLE) {
                // we will remove this field from the old document and later re-add the last such field to the new document
                singleCardinalityDeletions.add(indexEntry.field);
            } else if (cardinality == Cardinality.LIST || cardinality == Cardinality.SET) {
                // we collect a set of values that should be removed
                if (!setAndListCardinalityDeletions.containsKey(indexEntry.field)) {
                    setAndListCardinalityDeletions.put(indexEntry.field, new HashSet<>());
                }
                setAndListCardinalityDeletions.get(indexEntry.field).add(new FieldPair(indexEntry.field, indexEntry.value.toString()));
            } else throw new IllegalArgumentException("cardinality not supported: " + cardinality);
        }
        final Map<String, Set<FieldPair>> encounteredSetValues = new HashMap<>();

        // remove deleted values from the old restored document (and all Cardinality.Single values that are updated)
        final Iterator<IndexableField> iterator = doc.iterator();
        while (iterator.hasNext()) {
            final IndexableField storedField = iterator.next();
            final String fieldName = storedField.name();
            if (DOCID.equals(fieldName)) continue;

            final KeyInformation kinfo = informations.get(storename, fieldName);
            final Cardinality cardinality = kinfo.getCardinality();

            if (cardinality == Cardinality.SINGLE && !singleCardinalityDeletions.contains(fieldName)) continue;
            if (cardinality != Cardinality.SINGLE) {
                Set<FieldPair> set = setAndListCardinalityDeletions.get(fieldName);
                if (set == null || !set.contains(fieldPair(storedField))) {
                    if (cardinality == Cardinality.SET) {
                        // we collect encountered set values that aren't removed
                        if (!encounteredSetValues.containsKey(fieldName)) {
                            encounteredSetValues.put(fieldName, new HashSet<>());
                        }
                        encounteredSetValues.get(fieldName).add(fieldPair(storedField));
                    }
                    continue;
                }
            }
            iterator.remove();
            // for Cardinality.Single, we remove all fields with the same name in the additions and deletions
            // for other cardinality, we remove only fields that have the same name and the same value, in the deletions
            // potential issue, what if there are multiple instances of the same value in a field with Cardinality.LIST
            // but I'm not sure janus graph cares about that, given how IndexMutation is designed
        }

        // now handle the additions with respect to the cardinality
        // first handle single additions
        for (final Map.Entry<String, Object> entry:singleCardinalityAdditions.entrySet()) {
            doc.add(storedField(entry.getKey(), entry.getValue(), true, mappingFactory));
        }

        // next handle set and list additions
        for (final Map.Entry<String, List<IndexableField>> entry:listAndSetCardinalityGroupedAdditions.entrySet()) {
            final KeyInformation keyInformation = informations.get(storename, entry.getKey());
            final Cardinality cardinality = keyInformation.getCardinality();
            final String fieldName = entry.getKey();
            final List<IndexableField> list = entry.getValue();
            if (cardinality == Cardinality.LIST) {
                for (IndexableField indexableField : list) {
                    doc.add(indexableField);
                }
            } else {
                // cardinality == Cardinality.SET
                if (!encounteredSetValues.containsKey(fieldName)) encounteredSetValues.put(fieldName, new HashSet<>());
                final Set<FieldPair> set = encounteredSetValues.get(fieldName);
                for (IndexableField indexableField : list) {
                    // only add the field to the doc if it hasn't already been added
                    FieldPair fieldPair = fieldPair(indexableField);
                    if (!set.contains(fieldPair)) {
                        doc.add(indexableField);
                        set.add(fieldPair);
                    }
                }
            }
        }

        // at this point the document should only hold stored (persistent) IndexableFields
        // now add (non-persistent) fields that are used for lookups in the document (except for DOCID)
        // the copy is necessary to avoid concurrent modifications
        final List<IndexableField> fields = doc.getFields();
        final List<IndexableField> copy = new ArrayList<>(fields.size());
        copy.addAll(fields);
        for (IndexableField field : copy) {
            if (!field.name().equals(DOCID)) addNonPersistantField(doc, storename, field, informations);
        }

        writer.updateDocument(new Term(DOCID, docid), doc);
    }

    // we should be able to restore all fields (except DOCID) from an old document, which means they must have a stored counterpart
    private IndexableField storedField(final String fieldName, final Object value, boolean isCardinalitySingle, final Function<String, Mapping> mappingFactory) {
        if (value instanceof Number) {
            if (AttributeUtil.isWholeNumber((Number) value)) {
                return new StoredField(fieldName, ((Number) value).longValue());
            } else { //double or float
                return new StoredField(fieldName, ((Number) value).doubleValue());
            }
        } else if (AttributeUtil.isString(value)) {
            String str = (String) value;
            // we only need a mapping when we have a String value
            Mapping mapping = mappingFactory.apply(fieldName);
            switch (mapping) {
                case DEFAULT:
                case TEXT:
                    return new TextField(fieldName, str.toLowerCase(), Field.Store.YES);
                case STRING:
                    return new TextField(fieldName, str, Field.Store.YES);

                default:
                    throw new IllegalArgumentException("Illegal mapping specified: " + mapping);
            }
        } else if (value instanceof Geoshape) {
            return new StoredField(fieldName, GEOID + value.toString());
        } else if (value instanceof Date) {
            return new StoredField(fieldName, (((Date) value).getTime()));
        } else if (value instanceof Instant) {
            return new StoredField(fieldName, (((Instant) value).toEpochMilli()));
        } else if (value instanceof Boolean) {
            return new StoredField(fieldName, ((Boolean) value) ? 1 : 0);
        } else if (value instanceof UUID) {
            // We store UUID as TextField analyzed with the KeywordAnalyzer
            // With the current design of LuceneIndex, this removes complexity when making sure
            // that values are not lost when mutating the index
            return new TextField(fieldName, value.toString(), Field.Store.YES);
        } else {
            throw new IllegalArgumentException("Unsupported type for field " + fieldName + " with value :" + value + "and class " + value.getClass());
        }
    }


    /**
     * compute a field (for lookup purposes) from a restorable field (or return null if it is not necessary)
     */
    private void addNonPersistantField(final Document doc, final String store, final IndexableField field, final KeyInformation.IndexRetriever informations) {
        final String fieldName = field.name();
        final KeyInformation keyInformation = informations.get(store, fieldName);
        final Class clazz = keyInformation.getDataType();
        if (Number.class.isAssignableFrom(clazz)) {
            if (AttributeUtil.isWholeNumber(clazz)) {
                long value = field.numericValue().longValue();
                if (keyInformation.getCardinality() == Cardinality.SINGLE) {
                    doc.add(new NumericDocValuesField(fieldName, value));
                }
                doc.add(new LongPoint(fieldName, value));
            } else {
                double value = field.numericValue().doubleValue();
                if (keyInformation.getCardinality() == Cardinality.SINGLE) {
                    doc.add(new DoubleDocValuesField(fieldName, value));
                }
                doc.add(new DoublePoint(fieldName, value));
            }
        } else if (AttributeUtil.isString(clazz)) {
            // string fields are stored as TextField, no need to add stuff
        } else if (Geoshape.class.isAssignableFrom(clazz)) {
            final Shape shape;
            try {
                shape = Geoshape.fromWkt(field.stringValue().substring(GEOID.length())).getShape();
            } catch (java.text.ParseException e) {
                throw new IllegalArgumentException("Geoshape was unparsable", e);
            }
            final SpatialStrategy spatialStrategy = getSpatialStrategy(fieldName, keyInformation);

            for (Field f : spatialStrategy.createIndexableFields(shape)) {
                doc.add(f);
            }
        } else if (Date.class.isAssignableFrom(clazz)) {
            doc.add(new LongPoint(fieldName, field.numericValue().longValue()));
        } else if (Instant.class.isAssignableFrom(clazz)) {
            doc.add(new LongPoint(fieldName, field.numericValue().longValue()));
        } else if (Boolean.class.isAssignableFrom(clazz)) {
            doc.add(new IntPoint(fieldName, field.numericValue().intValue()));
        } else if (UUID.class.isAssignableFrom(clazz)) {
            // nothing to do, this is stored as a TextField
        } else {
            throw new IllegalArgumentException("Unsupported type for class: " + clazz);
        }
    }

    @Override
    public void restore(Map<String, Map<String, List<IndexEntry>>> documents, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {
        writerLock.lock();
        try {
            for (final Map.Entry<String, Map<String, List<IndexEntry>>> stores : documents.entrySet()) {
                final String store = stores.getKey();
                final IndexWriter writer = getWriter(store, information);
                final IndexReader reader = DirectoryReader.open(writer, true, true);
                final IndexSearcher searcher = new IndexSearcher(reader);

                for (final Map.Entry<String, List<IndexEntry>> entry : stores.getValue().entrySet()) {
                    final String docID = entry.getKey();
                    final List<IndexEntry> content = entry.getValue();

                    if (content == null || content.isEmpty()) {
                        if (log.isTraceEnabled())
                            log.trace("Deleting document [{}]", docID);

                        writer.deleteDocuments(new Term(DOCID, docID));
                        continue;
                    }

                    updateDocumentForIndexing(writer, searcher, store, information, docID, false, Collections.emptyList(), content);
                }
                writer.commit();
            }
            tx.commit();
        } catch (final IOException e) {
            throw new TemporaryBackendException("Could not update Lucene index", e);
        } finally {
            writerLock.unlock();
        }
    }

    private static Sort getSortOrder(IndexQuery query) {
        final Sort sort = new Sort();
        final List<IndexQuery.OrderEntry> orders = query.getOrder();
        if (!orders.isEmpty()) {
            final SortField[] fields = new SortField[orders.size()];
            for (int i = 0; i < orders.size(); i++) {
                final IndexQuery.OrderEntry order = orders.get(i);
                SortField.Type sortType = null;
                final Class dataType = order.getDatatype();
                if (AttributeUtil.isString(dataType)) sortType = SortField.Type.STRING;
                else if (AttributeUtil.isWholeNumber(dataType)) sortType = SortField.Type.LONG;
                else if (AttributeUtil.isDecimal(dataType)) sortType = SortField.Type.DOUBLE;
                else
                    Preconditions.checkArgument(false, "Unsupported order specified on field [%s] with datatype [%s]", order.getKey(), dataType);

                fields[i] = new SortField(order.getKey(), sortType, order.getOrder() == Order.DESC);
            }
            sort.setSort(fields);
        }
        return sort;
    }

    @Override
    public Stream<String> query(IndexQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {
        //Construct query
        final String store = query.getStore();
        final LuceneCustomAnalyzer delegatingAnalyzer = delegatingAnalyzerFor(store, information);
        final SearchParams searchParams = convertQuery(query.getCondition(), information.get(store), delegatingAnalyzer);

        try {
            final IndexSearcher searcher = ((Transaction) tx).getSearcher(query.getStore());
            if (searcher == null) {
                return Collections.unmodifiableList(new ArrayList<String>()).stream(); //Index does not yet exist
            }
            Query q = searchParams.getQuery();
            if (null == q)
                q = new MatchAllDocsQuery();

            final long time = System.currentTimeMillis();
            final TopDocs docs = searcher.search(q, query.hasLimit() ? query.getLimit() : Integer.MAX_VALUE - 1, getSortOrder(query));
            log.debug("Executed query [{}] in {} ms", q, System.currentTimeMillis() - time);
            final List<String> result = new ArrayList<>(docs.scoreDocs.length);
            for (int i = 0; i < docs.scoreDocs.length; i++) {
                final IndexableField field = searcher.doc(docs.scoreDocs[i].doc).getField(DOCID);
                result.add(field == null ? null : field.stringValue());
            }
            return result.stream();
        } catch (final IOException e) {
            throw new TemporaryBackendException("Could not execute Lucene query", e);
        }
    }

    private static Query numericQuery(String key, Cmp relation, Number value) {
        switch (relation) {
            case EQUAL:
                return AttributeUtil.isWholeNumber(value) ?
                    LongPoint.newRangeQuery(key, value.longValue(), value.longValue()) :
                    DoublePoint.newRangeQuery(key, value.doubleValue(), value.doubleValue());
            case NOT_EQUAL:
                final BooleanQuery.Builder q = new BooleanQuery.Builder();
                if (AttributeUtil.isWholeNumber(value)) {
                    q.add(LongPoint.newRangeQuery(key, Long.MIN_VALUE, Math.addExact(value.longValue(), -1)), BooleanClause.Occur.SHOULD);
                    q.add(LongPoint.newRangeQuery(key, Math.addExact(value.longValue(), 1), Long.MAX_VALUE), BooleanClause.Occur.SHOULD);
                } else {
                    q.add(DoublePoint.newRangeQuery(key, Double.MIN_VALUE, DoublePoint.nextDown(value.doubleValue())), BooleanClause.Occur.SHOULD);
                    q.add(DoublePoint.newRangeQuery(key, DoublePoint.nextUp(value.doubleValue()), Double.MAX_VALUE), BooleanClause.Occur.SHOULD);
                }
                return q.build();
            case LESS_THAN:
                return (AttributeUtil.isWholeNumber(value)) ?
                    LongPoint.newRangeQuery(key, Long.MIN_VALUE, Math.addExact(value.longValue(), -1)) :
                    DoublePoint.newRangeQuery(key, Double.MIN_VALUE, DoublePoint.nextDown(value.doubleValue()));
            case LESS_THAN_EQUAL:
                return (AttributeUtil.isWholeNumber(value)) ?
                    LongPoint.newRangeQuery(key, Long.MIN_VALUE, value.longValue()) :
                    DoublePoint.newRangeQuery(key, Double.MIN_VALUE, value.doubleValue());
            case GREATER_THAN:
                return (AttributeUtil.isWholeNumber(value)) ?
                    LongPoint.newRangeQuery(key, Math.addExact(value.longValue(), 1), Long.MAX_VALUE) :
                    DoublePoint.newRangeQuery(key, DoublePoint.nextUp(value.doubleValue()), Double.MAX_VALUE);
            case GREATER_THAN_EQUAL:
                return (AttributeUtil.isWholeNumber(value)) ?
                    LongPoint.newRangeQuery(key, value.longValue(), Long.MAX_VALUE) :
                    DoublePoint.newRangeQuery(key, value.doubleValue(), Double.MAX_VALUE);
            default:
                throw new IllegalArgumentException("Unexpected relation: " + relation);
        }
    }

    // adapted from SolrIndex
    private List<String> customTokenize(Analyzer analyzer, String fieldName, String value) {
        final List<String> terms = new ArrayList<>();
        try (CachingTokenFilter stream = new CachingTokenFilter(analyzer.tokenStream(fieldName, value))) {
            final TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                terms.add(termAtt.getBytesRef().utf8ToString());
            }
            return terms;
        } catch (IOException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    private void tokenize(SearchParams params, final Mapping mapping, final LuceneCustomAnalyzer delegatingAnalyzer, String value, String key, JanusGraphPredicate janusgraphPredicate) {
        final Analyzer analyzer = delegatingAnalyzer.getWrappedAnalyzer(key);
        final List<String> terms = customTokenize(analyzer, key, value);
        if (terms.size() == 1) {
            if (janusgraphPredicate == Cmp.EQUAL || janusgraphPredicate == Text.CONTAINS) {
                params.addQuery(new TermQuery(new Term(key, value)));
            } else if (janusgraphPredicate == Cmp.NOT_EQUAL) {
                final BooleanQuery.Builder q = new BooleanQuery.Builder();
                q.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
                q.add(new TermQuery(new Term(key, value)), BooleanClause.Occur.MUST_NOT);
                params.addQuery(q.build(), BooleanClause.Occur.MUST);
            } else if (janusgraphPredicate == Text.CONTAINS_PREFIX) {
                final Term term = new Term(key, terms.get(0).toLowerCase());
                params.addQuery(new PrefixQuery(term), BooleanClause.Occur.MUST);
            } else
                throw new IllegalArgumentException("LuceneIndex does not support this predicate with 1 token : " + janusgraphPredicate);
        } else {
            // at the moment, this is only walked for EQUAL and Text.CONTAINS (String and Text mappings)
            final BooleanQuery.Builder q = new BooleanQuery.Builder();
            for (final String term : terms) {
                q.add(new TermQuery(new Term(key, term)), BooleanClause.Occur.MUST);
            }
            params.addQuery(q.build());
        }
    }

    private LuceneCustomAnalyzer delegatingAnalyzerFor(String store, KeyInformation.IndexRetriever information2) {
        if (!delegatingAnalyzers.containsKey(store)) {
            delegatingAnalyzers.put(store, new LuceneCustomAnalyzer(store, information2, Analyzer.PER_FIELD_REUSE_STRATEGY));
        }
        return delegatingAnalyzers.get(store);
    }

    private SearchParams convertQuery(Condition<?> condition, final KeyInformation.StoreRetriever information, final LuceneCustomAnalyzer delegatingAnalyzer) {
        final SearchParams params = new SearchParams();
        if (condition instanceof PredicateCondition) {
            final PredicateCondition<String, ?> atom = (PredicateCondition) condition;
            Object value = atom.getValue();
            final String key = atom.getKey();
            final JanusGraphPredicate janusgraphPredicate = atom.getPredicate();
            if (value instanceof Number) {
                Preconditions.checkArgument(janusgraphPredicate instanceof Cmp, "Relation not supported on numeric types: " + janusgraphPredicate);
                params.addQuery(numericQuery(key, (Cmp) janusgraphPredicate, (Number) value));
            } else if (value instanceof String) {
                if (janusgraphPredicate == Cmp.LESS_THAN) {
                    params.addQuery(TermRangeQuery.newStringRange(key, null, value.toString(), false, false));
                } else if (janusgraphPredicate == Cmp.LESS_THAN_EQUAL) {
                    params.addQuery(TermRangeQuery.newStringRange(key, null, value.toString(), false, true));
                } else if (janusgraphPredicate == Cmp.GREATER_THAN) {
                    params.addQuery(TermRangeQuery.newStringRange(key, value.toString(), null, false, false));
                } else if (janusgraphPredicate == Cmp.GREATER_THAN_EQUAL) {
                    params.addQuery(TermRangeQuery.newStringRange(key, value.toString(), null, true, false));
                } else {
                    final Mapping map = Mapping.getMapping(information.get(key));
                    if ((map == Mapping.DEFAULT || map == Mapping.TEXT) && !Text.HAS_CONTAINS.contains(janusgraphPredicate))
                        throw new IllegalArgumentException("Text mapped string values only support CONTAINS queries and not: " + janusgraphPredicate);
                    if (map == Mapping.STRING && Text.HAS_CONTAINS.contains(janusgraphPredicate))
                        throw new IllegalArgumentException("String mapped string values do not support CONTAINS queries: " + janusgraphPredicate);
                    if (janusgraphPredicate == Text.CONTAINS) {
                        tokenize(params, map, delegatingAnalyzer, ((String) value).toLowerCase(), key, janusgraphPredicate);
                    } else if (janusgraphPredicate == Text.CONTAINS_PREFIX) {
                        tokenize(params, map, delegatingAnalyzer, (String) value, key, janusgraphPredicate);
                    } else if (janusgraphPredicate == Text.PREFIX) {
                        params.addQuery(new PrefixQuery(new Term(key, (String) value)));
                    } else if (janusgraphPredicate == Text.REGEX) {
                        final RegexpQuery rq = new RegexpQuery(new Term(key, (String) value));
                        params.addQuery(rq);
                    } else if (janusgraphPredicate == Text.CONTAINS_REGEX) {
                        // This is terrible -- there is probably a better way
                        // putting this to lowercase because Text search is supposed to be case insensitive
                        final RegexpQuery rq = new RegexpQuery(new Term(key, ".*" + (((String) value).toLowerCase()) + ".*"));
                        params.addQuery(rq);
                    } else if (janusgraphPredicate == Cmp.EQUAL) {
                        tokenize(params, map, delegatingAnalyzer, (String) value, key, janusgraphPredicate);
                    } else if (janusgraphPredicate == Cmp.NOT_EQUAL) {
                        final BooleanQuery.Builder q = new BooleanQuery.Builder();
                        q.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
                        q.add(new TermQuery(new Term(key, (String) value)), BooleanClause.Occur.MUST_NOT);
                        params.addQuery(q.build());
                    } else if (janusgraphPredicate == Text.FUZZY) {
                        params.addQuery(new FuzzyQuery(new Term(key, (String) value)));
                    } else if (janusgraphPredicate == Text.CONTAINS_FUZZY) {
                        value = ((String) value).toLowerCase();
                        final Builder b = new BooleanQuery.Builder();
                        for (final String term : Text.tokenize((String) value)) {
                            b.add(new FuzzyQuery(new Term(key, term)), BooleanClause.Occur.MUST);
                        }
                        params.addQuery(b.build());
                    } else
                        throw new IllegalArgumentException("Relation is not supported for string value: " + janusgraphPredicate);
                }
            } else if (value instanceof Geoshape) {
                Preconditions.checkArgument(janusgraphPredicate instanceof Geo, "Relation not supported on geo types: " + janusgraphPredicate);
                final Shape shape = ((Geoshape) value).getShape();
                final SpatialOperation spatialOp = SPATIAL_PREDICATES.get(janusgraphPredicate);
                final SpatialArgs args = new SpatialArgs(spatialOp, shape);
                params.addQuery(getSpatialStrategy(key, information.get(key)).makeQuery(args));
            } else if (value instanceof Date) {
                Preconditions.checkArgument(janusgraphPredicate instanceof Cmp, "Relation not supported on date types: " + janusgraphPredicate);
                params.addQuery(numericQuery(key, (Cmp) janusgraphPredicate, ((Date) value).getTime()));
            } else if (value instanceof Instant) {
                Preconditions.checkArgument(janusgraphPredicate instanceof Cmp, "Relation not supported on instant types: " + janusgraphPredicate);
                params.addQuery(numericQuery(key, (Cmp) janusgraphPredicate, ((Instant) value).toEpochMilli()));
            } else if (value instanceof Boolean) {
                Preconditions.checkArgument(janusgraphPredicate instanceof Cmp, "Relation not supported on boolean types: " + janusgraphPredicate);
                final int intValue;
                switch ((Cmp) janusgraphPredicate) {
                    case EQUAL:
                        intValue = ((Boolean) value) ? 1 : 0;
                        params.addQuery(IntPoint.newRangeQuery(key, intValue, intValue));
                        break;
                    case NOT_EQUAL:
                        intValue = ((Boolean) value) ? 0 : 1;
                        params.addQuery(IntPoint.newRangeQuery(key, intValue, intValue));
                        break;
                    default:
                        throw new IllegalArgumentException("Boolean types only support EQUAL or NOT_EQUAL");
                }

            } else if (value instanceof UUID) {
                Preconditions.checkArgument(janusgraphPredicate instanceof Cmp, "Relation not supported on UUID types: " + janusgraphPredicate);
                if (janusgraphPredicate == Cmp.EQUAL) {
                    params.addQuery(new TermQuery(new Term(key, value.toString())));
                } else if (janusgraphPredicate == Cmp.NOT_EQUAL) {
                    final BooleanQuery.Builder q = new BooleanQuery.Builder();
                    q.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
                    q.add(new TermQuery(new Term(key, value.toString())), BooleanClause.Occur.MUST_NOT);
                    params.addQuery(q.build());
                } else {
                    throw new IllegalArgumentException("Relation is not supported for UUID type: " + janusgraphPredicate);
                }

            } else {
                throw new IllegalArgumentException("Unsupported type: " + value);
            }
        } else if (condition instanceof Not) {
            final SearchParams childParams = convertQuery(((Not) condition).getChild(), information, delegatingAnalyzer);
            params.addQuery(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
            params.addParams(childParams, BooleanClause.Occur.MUST_NOT);
        } else if (condition instanceof And) {
            for (final Condition c : condition.getChildren()) {
                final SearchParams childParams = convertQuery(c, information, delegatingAnalyzer);
                params.addParams(childParams, BooleanClause.Occur.MUST);
            }
        } else if (condition instanceof Or) {
            for (final Condition c : condition.getChildren()) {
                final SearchParams childParams = convertQuery(c, information, delegatingAnalyzer);
                params.addParams(childParams, BooleanClause.Occur.SHOULD);
            }
        } else throw new IllegalArgumentException("Invalid condition: " + condition);
        return params;
    }

    @Override
    public Stream<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {
        final Query q;
        try {
            final Analyzer analyzer = delegatingAnalyzerFor(query.getStore(), information);//writers.get(query.getStore()).getAnalyzer();
            q = new QueryParser("_all", analyzer).parse(query.getQuery());
            // Lucene query parser does not take additional parameters so any parameters on the RawQuery are ignored. 
        } catch (final ParseException e) {
            throw new PermanentBackendException("Could not parse raw query: " + query.getQuery(), e);
        }

        try {
            final IndexSearcher searcher = ((Transaction) tx).getSearcher(query.getStore());
            if (searcher == null) {
                return Collections.unmodifiableList(new ArrayList<RawQuery.Result<String>>()).stream(); //Index does not yet exist
            }
            final long time = System.currentTimeMillis();
            //TODO: can we make offset more efficient in Lucene?
            final int offset = query.getOffset();
            int adjustedLimit = query.hasLimit() ? query.getLimit() : Integer.MAX_VALUE - 1;
            if (adjustedLimit < Integer.MAX_VALUE - 1 - offset) adjustedLimit += offset;
            else adjustedLimit = Integer.MAX_VALUE - 1;
            final TopDocs docs = searcher.search(q, adjustedLimit);
            log.debug("Executed query [{}] in {} ms", q, System.currentTimeMillis() - time);
            final List<RawQuery.Result<String>> result = new ArrayList<>(docs.scoreDocs.length);
            for (int i = offset; i < docs.scoreDocs.length; i++) {
                final IndexableField field = searcher.doc(docs.scoreDocs[i].doc).getField(DOCID);
                result.add(new RawQuery.Result<>(field == null ? null : field.stringValue(), docs.scoreDocs[i].score));
            }
            return result.stream();
        } catch (final IOException e) {
            throw new TemporaryBackendException("Could not execute Lucene query", e);
        }
    }

    @Override
    public Long totals(RawQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {
        final Query q;
        try {
            final Analyzer analyzer = delegatingAnalyzerFor(query.getStore(), information);//writers.get(query.getStore()).getAnalyzer();
            q = new QueryParser("_all", analyzer).parse(query.getQuery());
        } catch (final ParseException e) {
            throw new PermanentBackendException("Could not parse raw query: " + query.getQuery(), e);
        }

        try {
            final IndexSearcher searcher = ((Transaction) tx).getSearcher(query.getStore());
            if (searcher == null) return 0L; //Index does not yet exist

            final long time = System.currentTimeMillis();
            // Lucene doesn't like limits of 0.  Also, it doesn't efficiently build a total list.
            query.setLimit(1);
            // We ignore offset and limit for totals
            final TopDocs docs = searcher.search(q, 1);
            log.debug("Executed query [{}] in {} ms", q, System.currentTimeMillis() - time);
            return docs.totalHits;
        } catch (final IOException e) {
            throw new TemporaryBackendException("Could not execute Lucene query", e);
        }
    }

    @Override
    public BaseTransactionConfigurable beginTransaction(BaseTransactionConfig config) throws BackendException {
        return new Transaction(config);
    }

    @Override
    public boolean supports(KeyInformation information, JanusGraphPredicate janusgraphPredicate) {
        final Class<?> dataType = information.getDataType();
        final Mapping mapping = Mapping.getMapping(information);
        if (mapping != Mapping.DEFAULT && !AttributeUtil.isString(dataType) &&
            !(mapping == Mapping.PREFIX_TREE && AttributeUtil.isGeo(dataType))) return false;

        if (Number.class.isAssignableFrom(dataType)) {
            return janusgraphPredicate instanceof Cmp;
        } else if (dataType == Geoshape.class) {
            if (information.getCardinality() != Cardinality.SINGLE) return false;
            return janusgraphPredicate == Geo.INTERSECT || janusgraphPredicate == Geo.WITHIN || janusgraphPredicate == Geo.CONTAINS;
        } else if (AttributeUtil.isString(dataType)) {
            switch (mapping) {
                case DEFAULT:
                case TEXT:
                    return janusgraphPredicate == Text.CONTAINS || janusgraphPredicate == Text.CONTAINS_PREFIX || janusgraphPredicate == Text.CONTAINS_FUZZY; // || janusgraphPredicate == Text.CONTAINS_REGEX;
                case STRING:
                    return janusgraphPredicate instanceof Cmp || janusgraphPredicate == Text.PREFIX || janusgraphPredicate == Text.REGEX || janusgraphPredicate == Text.FUZZY;
            }
        } else if (dataType == Date.class || dataType == Instant.class) {
            return janusgraphPredicate instanceof Cmp;
        } else if (dataType == Boolean.class) {
            return janusgraphPredicate == Cmp.EQUAL || janusgraphPredicate == Cmp.NOT_EQUAL;
        } else if (dataType == UUID.class) {
            return janusgraphPredicate == Cmp.EQUAL || janusgraphPredicate == Cmp.NOT_EQUAL;
        }
        return false;
    }

    @Override
    public boolean supports(KeyInformation information) {
        final Class<?> dataType = information.getDataType();
        final Mapping mapping = Mapping.getMapping(information);
        if (Number.class.isAssignableFrom(dataType) || dataType == Date.class || dataType == Instant.class || dataType == Boolean.class || dataType == UUID.class) {
            return mapping == Mapping.DEFAULT;
        } else if (AttributeUtil.isString(dataType)) {
            return mapping == Mapping.DEFAULT || mapping == Mapping.STRING || mapping == Mapping.TEXT;
        } else if (AttributeUtil.isGeo(dataType)) {
            return mapping == Mapping.DEFAULT || mapping == Mapping.PREFIX_TREE;
        }
        return false;
    }

    @Override
    public String mapKey2Field(String key, KeyInformation information) {
        Preconditions.checkArgument(!StringUtils.containsAny(key, new char[]{' '}), "Invalid key name provided: %s", key);
        return key;
    }

    @Override
    public IndexFeatures getFeatures() {
        return LUCENE_FEATURES;
    }

    @Override
    public void close() throws BackendException {
        try {
            for (IndexWriter w : writers.values()) w.close();
        } catch (IOException e) {
            throw new PermanentBackendException("Could not close writers", e);
        }
    }

    @Override
    public void clearStorage() throws BackendException {
        try {
            FileUtils.deleteDirectory(new File(basePath));
        } catch (final IOException e) {
            throw new PermanentBackendException("Could not delete lucene directory: " + basePath, e);
        }
    }

    @Override
    public boolean exists() throws BackendException {
        if (Files.exists(Paths.get(basePath))) {
            try (final DirectoryStream<Path> dirStream = Files.newDirectoryStream(Paths.get(basePath))) {
                return dirStream.iterator().hasNext();
            } catch (final IOException e) {
                throw new PermanentBackendException("Could not read lucene directory: " + basePath, e);
            }
        } else {
            return false;
        }
    }

    private class Transaction implements BaseTransactionConfigurable {

        private final BaseTransactionConfig config;
        private final Set<String> updatedStores = Sets.newHashSet();
        private final Map<String, IndexSearcher> searchers = new HashMap<>(4);

        private Transaction(BaseTransactionConfig config) {
            this.config = config;
        }

        private synchronized IndexSearcher getSearcher(String store) throws BackendException {
            IndexSearcher searcher = searchers.get(store);
            if (searcher == null) {
                final IndexReader reader;
                try {
                    reader = DirectoryReader.open(getStoreDirectory(store));
                    searcher = new IndexSearcher(reader);
                } catch (final IndexNotFoundException e) {
                    searcher = null;
                } catch (final IOException e) {
                    throw new PermanentBackendException("Could not open index reader on store: " + store, e);
                }
                searchers.put(store, searcher);
            }
            return searcher;
        }

        public void postCommit() throws BackendException {
            close();
            searchers.clear();
        }


        @Override
        public void commit() throws BackendException {
            close();
        }

        @Override
        public void rollback() throws BackendException {
            close();
        }

        private void close() throws BackendException {
            try {
                for (final IndexSearcher searcher : searchers.values()) {
                    if (searcher != null) searcher.getIndexReader().close();
                }
            } catch (final IOException e) {
                throw new PermanentBackendException("Could not close searcher", e);
            }
        }

        @Override
        public BaseTransactionConfig getConfiguration() {
            return config;
        }
    }

    /**
     * Encapsulates a Lucene Query that express a JanusGraph {@link org.janusgraph.graphdb.query.Query} using Lucene's
     * abstractions. This object's state is mutable.
     */
    private static class SearchParams {
        private final BooleanQuery.Builder qb = new BooleanQuery.Builder();

        private void addQuery(Query newQuery) {
            addQuery(newQuery, BooleanClause.Occur.MUST);
        }

        private void addQuery(Query newQuery, BooleanClause.Occur occur) {
            qb.add(newQuery, occur);
        }

        private void addParams(SearchParams other, BooleanClause.Occur occur) {
            final Query otherQuery = other.getQuery();
            if (null != otherQuery)
                addQuery(otherQuery, occur);
        }

        private Query getQuery() {
            final BooleanQuery q = qb.build();
            if (0 == q.clauses().size()) {
                return null;
            }
            return q;
        }
    }

    private static class FieldPair {
        public final String fieldName;
        public final String value;

        public FieldPair(String field, String value) {
            this.fieldName = field;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final FieldPair that = (FieldPair) o;

            if (fieldName != null ? !fieldName.equals(that.fieldName) : that.fieldName != null) return false;
            return value != null ? value.equals(that.value) : that.value == null;
        }

        @Override
        public int hashCode() {
            int result = fieldName != null ? fieldName.hashCode() : 0;
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "FieldPair{" +
                "fieldName='" + fieldName + '\'' +
                ", value='" + value + '\'' +
                '}';
        }
    }
}
