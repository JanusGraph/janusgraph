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
import com.google.common.collect.Maps;
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
import org.janusgraph.util.system.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
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

    private static Map<Geo, SpatialOperation> SPATIAL_PREDICATES = spatialPredicates();

    private final Analyzer analyzer = new StandardAnalyzer();

    private final Map<String, IndexWriter> writers = new HashMap<String, IndexWriter>(4);
    private final ReentrantLock writerLock = new ReentrantLock();

    private Map<String, SpatialStrategy> spatial = new ConcurrentHashMap<String, SpatialStrategy>(12);
    private SpatialContext ctx = Geoshape.getSpatialContext();

    private final String basePath;

    public LuceneIndex(Configuration config) {
        String dir = config.get(GraphDatabaseConfiguration.INDEX_DIRECTORY);
        File directory = new File(dir);
        if ((!directory.exists() && !directory.mkdirs()) || !directory.isDirectory() || !directory.canWrite()) {
            throw new IllegalArgumentException("Cannot access or write to directory: " + dir);
        }
        basePath = directory.getAbsolutePath();
        log.debug("Configured Lucene to use base directory [{}]", basePath);
    }

    private Directory getStoreDirectory(String store) throws BackendException {
        Preconditions.checkArgument(StringUtils.isAlphanumeric(store), "Invalid store name: %s", store);
        String dir = basePath + File.separator + store;
        try {
            File path = new File(dir);
            if ((!path.exists() && !path.mkdirs()) || !path.isDirectory() || !path.canWrite()) {
                throw new PermanentBackendException("Cannot access or write to directory: " + dir);
            }
            log.debug("Opening store directory [{}]", path);
            return FSDirectory.open(path.toPath());
        } catch (IOException e) {
            throw new PermanentBackendException("Could not open directory: " + dir, e);
        }
    }

    private IndexWriter getWriter(String store) throws BackendException {
        Preconditions.checkArgument(writerLock.isHeldByCurrentThread());
        IndexWriter writer = writers.get(store);
        if (writer == null) {
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            try {
                writer = new IndexWriter(getStoreDirectory(store), iwc);
                writers.put(store, writer);
            } catch (IOException e) {
                throw new PermanentBackendException("Could not create writer", e);
            }
        }
        return writer;
    }

    private SpatialStrategy getSpatialStrategy(String key, KeyInformation ki) {
        SpatialStrategy strategy = spatial.get(key);
        Mapping mapping = Mapping.getMapping(ki);
        int maxLevels = (int) ParameterType.INDEX_GEO_MAX_LEVELS.findParameter(ki.getParameters(), DEFAULT_GEO_MAX_LEVELS);
        double distErrorPct = (double) ParameterType.INDEX_GEO_DIST_ERROR_PCT.findParameter(ki.getParameters(), DEFAULT_GEO_DIST_ERROR_PCT);
        if (strategy == null) {
            synchronized (spatial) {
                if (!spatial.containsKey(key)) {
//                    SpatialPrefixTree grid = new GeohashPrefixTree(ctx, GEO_MAX_LEVELS);
//                    strategy = new RecursivePrefixTreeStrategy(grid, key);
                    if (mapping == Mapping.DEFAULT) {
                        strategy = PointVectorStrategy.newInstance(ctx, key);
                    } else {
                        SpatialPrefixTree grid = new QuadPrefixTree(ctx, maxLevels);
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
                .collect(Collectors.toMap((e) -> e.getKey(), (e) -> e.getValue())));
    }

    @Override
    public void register(String store, String key, KeyInformation information, BaseTransaction tx) throws BackendException {
        Class<?> dataType = information.getDataType();
        Mapping map = Mapping.getMapping(information);
        Preconditions.checkArgument(map == Mapping.DEFAULT || AttributeUtil.isString(dataType) ||
                (map == Mapping.PREFIX_TREE && AttributeUtil.isGeo(dataType)),
                "Specified illegal mapping [%s] for data type [%s]", map, dataType);
    }

    @Override
    public void mutate(Map<String, Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        Transaction ltx = (Transaction) tx;
        writerLock.lock();
        try {
            for (Map.Entry<String, Map<String, IndexMutation>> stores : mutations.entrySet()) {
                mutateStores(stores, informations);
            }
            ltx.postCommit();
        } catch (IOException e) {
            throw new TemporaryBackendException("Could not update Lucene index", e);
        } finally {
            writerLock.unlock();
        }
    }

    private void mutateStores(Map.Entry<String, Map<String, IndexMutation>> stores, KeyInformation.IndexRetriever informations) throws IOException, BackendException {
        IndexReader reader = null;
        try {
            String storename = stores.getKey();
            IndexWriter writer = getWriter(storename);
            reader = DirectoryReader.open(writer, true, true);
            IndexSearcher searcher = new IndexSearcher(reader);
            for (Map.Entry<String, IndexMutation> entry : stores.getValue().entrySet()) {
                String docid = entry.getKey();
                IndexMutation mutation = entry.getValue();

                if (mutation.isDeleted()) {
                    if (log.isTraceEnabled())
                        log.trace("Deleted entire document [{}]", docid);

                    writer.deleteDocuments(new Term(DOCID, docid));
                    continue;
                }

                Pair<Document, Map<String, Shape>> docAndGeo = retrieveOrCreate(docid, searcher);
                Document doc = docAndGeo.getKey();
                Map<String, Shape> geofields = docAndGeo.getValue();

                Preconditions.checkNotNull(doc);
                for (IndexEntry del : mutation.getDeletions()) {
                    Preconditions.checkArgument(!del.hasMetaData(), "Lucene index does not support indexing meta data: %s", del);
                    String key = del.field;
                    if (doc.getField(key) != null) {
                        if (log.isTraceEnabled())
                            log.trace("Removing field [{}] on document [{}]", key, docid);

                        doc.removeFields(key);
                        geofields.remove(key);
                    }
                }

                addToDocument(storename, docid, doc, mutation.getAdditions(), geofields, informations);

                //write the old document to the index with the modifications
                writer.updateDocument(new Term(DOCID, docid), doc);
            }
            writer.commit();
        } finally {
            IOUtils.closeQuietly(reader);
        }
    }

    @Override
    public void restore(Map<String, Map<String, List<IndexEntry>>> documents, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        writerLock.lock();
        try {
            for (Map.Entry<String, Map<String, List<IndexEntry>>> stores : documents.entrySet()) {
                String store = stores.getKey();
                IndexWriter writer = getWriter(store);
                IndexReader reader = DirectoryReader.open(writer, true, true);
                IndexSearcher searcher = new IndexSearcher(reader);

                for (Map.Entry<String, List<IndexEntry>> entry : stores.getValue().entrySet()) {
                    String docID = entry.getKey();
                    List<IndexEntry> content = entry.getValue();

                    if (content == null || content.isEmpty()) {
                        if (log.isTraceEnabled())
                            log.trace("Deleting document [{}]", docID);

                        writer.deleteDocuments(new Term(DOCID, docID));
                        continue;
                    }

                    Pair<Document, Map<String, Shape>> docAndGeo = retrieveOrCreate(docID, searcher);
                    addToDocument(store, docID, docAndGeo.getKey(), content, docAndGeo.getValue(), informations);

                    //write the old document to the index with the modifications
                    writer.updateDocument(new Term(DOCID, docID), docAndGeo.getKey());
                }
                writer.commit();
            }
            tx.commit();
        } catch (IOException e) {
            throw new TemporaryBackendException("Could not update Lucene index", e);
        } finally {
            writerLock.unlock();
        }
    }

    private Pair<Document, Map<String, Shape>> retrieveOrCreate(String docID, IndexSearcher searcher) throws IOException {
        Document doc;
        TopDocs hits = searcher.search(new TermQuery(new Term(DOCID, docID)), 10);
        Map<String, Shape> geofields = Maps.newHashMap();

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
            for (IndexableField field : doc.getFields()) {
                if (field.stringValue().startsWith(GEOID)) {
                    try {
                        geofields.put(field.name(), Geoshape.fromWkt(field.stringValue().substring(GEOID.length())).getShape());
                    } catch (java.text.ParseException e) {
                        throw new IllegalArgumentException("Geoshape was unparsable");
                    }
                }
            }
        }

        return new ImmutablePair<Document, Map<String, Shape>>(doc, geofields);
    }

    private void addToDocument(String store,
                               String docID,
                               Document doc,
                               List<IndexEntry> content,
                               Map<String, Shape> geofields,
                               KeyInformation.IndexRetriever informations) {
        Preconditions.checkNotNull(doc);
        for (IndexEntry e : content) {
            Preconditions.checkArgument(!e.hasMetaData(),"Lucene index does not support indexing meta data: %s",e);
            if (log.isTraceEnabled())
                log.trace("Adding field [{}] on document [{}]", e.field, docID);

            if (doc.getField(e.field) != null)
                doc.removeFields(e.field);

            if (e.value instanceof Number) {
                Field field;
                Field sortField;
                if (AttributeUtil.isWholeNumber((Number) e.value)) {
                    field = new LongPoint(e.field, ((Number) e.value).longValue());
                    sortField = new NumericDocValuesField(e.field, ((Number) e.value).longValue());
                } else { //double or float
                    field = new DoublePoint(e.field, ((Number) e.value).doubleValue());
                    sortField = new DoubleDocValuesField(e.field, ((Number) e.value).doubleValue());
                }
                doc.add(field);
                doc.add(sortField);
            } else if (AttributeUtil.isString(e.value)) {
                String str = (String) e.value;
                Mapping mapping = Mapping.getMapping(store, e.field, informations);
                Field field;
                switch(mapping) {
                    case DEFAULT:
                    case TEXT:
                        field = new TextField(e.field, str, Field.Store.YES);
                        break;
                    case STRING:
                        field = new StringField(e.field, str, Field.Store.YES);
                        break;
                    default: throw new IllegalArgumentException("Illegal mapping specified: " + mapping);
                }
                doc.add(field);
            } else if (e.value instanceof Geoshape) {
                Shape shape = ((Geoshape) e.value).getShape();
                geofields.put(e.field, shape);
                doc.add(new StoredField(e.field, GEOID +  e.value.toString()));
            } else if (e.value instanceof Date) {
                doc.add(new LongPoint(e.field, (((Date) e.value).getTime())));
            } else if (e.value instanceof Instant) {
                doc.add(new LongPoint(e.field, (((Instant) e.value).toEpochMilli())));
            } else if (e.value instanceof Boolean) {
                doc.add(new IntPoint(e.field, ((Boolean)e.value)? 1 : 0));
            } else if (e.value instanceof UUID) {
                //Solr stores UUIDs as strings, we we do the same.
                Field field = new StringField(e.field, e.value.toString(), Field.Store.YES);
                doc.add(field);
            } else {
                throw new IllegalArgumentException("Unsupported type: " + e.value);
            }
        }

        for (Map.Entry<String, Shape> geo : geofields.entrySet()) {
            if (log.isTraceEnabled())
                log.trace("Updating geo-indexes for key {}", geo.getKey());

            KeyInformation ki = informations.get(store, geo.getKey());
            SpatialStrategy spatialStrategy = getSpatialStrategy(geo.getKey(), ki);
            for (IndexableField f : spatialStrategy.createIndexableFields(geo.getValue())) {
                if (doc.getField(f.name()) != null) {
                    doc.removeFields(f.name());
                }
                doc.add(f);
                if (spatialStrategy instanceof PointVectorStrategy) {
                    doc.add(new DoubleDocValuesField(f.name(), f.numericValue() == null ? null : f.numericValue().doubleValue()));
                }
            }
        }
    }

    private static Sort getSortOrder(IndexQuery query) {
        Sort sort = new Sort();
        List<IndexQuery.OrderEntry> orders = query.getOrder();
        if (!orders.isEmpty()) {
            SortField[] fields = new SortField[orders.size()];
            for (int i = 0; i < orders.size(); i++) {
                IndexQuery.OrderEntry order = orders.get(i);
                SortField.Type sortType = null;
                Class datatype = order.getDatatype();
                if (AttributeUtil.isString(datatype)) sortType = SortField.Type.STRING;
                else if (AttributeUtil.isWholeNumber(datatype)) sortType = SortField.Type.LONG;
                else if (AttributeUtil.isDecimal(datatype)) sortType = SortField.Type.DOUBLE;
                else
                    Preconditions.checkArgument(false, "Unsupported order specified on field [%s] with datatype [%s]", order.getKey(), datatype);

                fields[i] = new SortField(order.getKey(), sortType, order.getOrder() == Order.DESC);
            }
            sort.setSort(fields);
        }
        return sort;
    }

    @Override
    public Stream<String> query(IndexQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        //Construct query
        SearchParams searchParams = convertQuery(query.getCondition(),informations.get(query.getStore()));

        try {
            IndexSearcher searcher = ((Transaction) tx).getSearcher(query.getStore());
            if (searcher == null) return Collections.unmodifiableList(new ArrayList<String>()).stream(); //Index does not yet exist

            Query q = searchParams.getQuery();
            if (null == q)
                q = new MatchAllDocsQuery();

            long time = System.currentTimeMillis();
            final TopDocs docs = searcher.search(q, query.hasLimit() ? query.getLimit() : Integer.MAX_VALUE - 1, getSortOrder(query));
            log.debug("Executed query [{}] in {} ms", q, System.currentTimeMillis() - time);
            List<String> result = new ArrayList<String>(docs.scoreDocs.length);
            for (int i = 0; i < docs.scoreDocs.length; i++) {
                final IndexableField field = searcher.doc(docs.scoreDocs[i].doc).getField(DOCID);
                result.add(field == null ? null : field.stringValue());
            }
            return result.stream();
        } catch (IOException e) {
            throw new TemporaryBackendException("Could not execute Lucene query", e);
        }
    }

    private static final Query numericQuery(String key, Cmp relation, Number value) {
        switch (relation) {
            case EQUAL:
                return AttributeUtil.isWholeNumber(value) ?
                        LongPoint.newRangeQuery(key, value.longValue(), value.longValue()) :
                    DoublePoint.newRangeQuery(key, value.doubleValue(), value.doubleValue());
            case NOT_EQUAL:
                BooleanQuery.Builder q = new BooleanQuery.Builder();
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

    private final SearchParams convertQuery(Condition<?> condition, KeyInformation.StoreRetriever informations) {
        SearchParams params = new SearchParams();
        if (condition instanceof PredicateCondition) {
            PredicateCondition<String, ?> atom = (PredicateCondition) condition;
            Object value = atom.getValue();
            String key = atom.getKey();
            JanusGraphPredicate janusgraphPredicate = atom.getPredicate();
            if (value instanceof Number) {
                Preconditions.checkArgument(janusgraphPredicate instanceof Cmp, "Relation not supported on numeric types: " + janusgraphPredicate);
                params.addQuery(numericQuery(key, (Cmp) janusgraphPredicate, (Number) value));
            } else if (value instanceof String) {
                Mapping map = Mapping.getMapping(informations.get(key));
                if ((map==Mapping.DEFAULT || map==Mapping.TEXT) && !Text.HAS_CONTAINS.contains(janusgraphPredicate))
                    throw new IllegalArgumentException("Text mapped string values only support CONTAINS queries and not: " + janusgraphPredicate);
                if (map==Mapping.STRING && Text.HAS_CONTAINS.contains(janusgraphPredicate))
                    throw new IllegalArgumentException("String mapped string values do not support CONTAINS queries: " + janusgraphPredicate);



                if (janusgraphPredicate == Text.CONTAINS) {
                    value = ((String) value).toLowerCase();
                    BooleanQuery.Builder b = new BooleanQuery.Builder();
                    for (String term : Text.tokenize((String)value)) {
                        b.add(new TermQuery(new Term(key, term)), BooleanClause.Occur.MUST);
                    }
                    params.addQuery(b.build());
                } else if (janusgraphPredicate == Text.CONTAINS_PREFIX) {
                    value = ((String) value).toLowerCase();
                    params.addQuery(new PrefixQuery(new Term(key, (String) value)));
                } else if (janusgraphPredicate == Text.PREFIX) {
                    params.addQuery(new PrefixQuery(new Term(key, (String) value)));
                } else if (janusgraphPredicate == Text.REGEX) {
                    RegexpQuery rq = new RegexpQuery(new Term(key, (String) value));
                    params.addQuery(rq);
                } else if (janusgraphPredicate == Text.CONTAINS_REGEX) {
                    // This is terrible -- there is probably a better way
                    RegexpQuery rq = new RegexpQuery(new Term(key, ".*" + (value) + ".*"));
                    params.addQuery(rq);
                } else if (janusgraphPredicate == Cmp.EQUAL) {
                    params.addQuery(new TermQuery(new Term(key,(String)value)));
                } else if (janusgraphPredicate == Cmp.NOT_EQUAL) {
                    BooleanQuery.Builder q = new BooleanQuery.Builder();
                    q.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
                    q.add(new TermQuery(new Term(key, (String) value)), BooleanClause.Occur.MUST_NOT);
                    params.addQuery(q.build());
                } else if(janusgraphPredicate == Text.FUZZY){
                    params.addQuery(new FuzzyQuery(new Term(key, (String) value)));
                } else if(janusgraphPredicate == Text.CONTAINS_FUZZY){
                    value = ((String) value).toLowerCase();
                    Builder b = new BooleanQuery.Builder();
                    for (String term : Text.tokenize((String)value)) {
                        b.add(new FuzzyQuery(new Term(key, term)),BooleanClause.Occur.MUST);
                    }
                    params.addQuery(b.build());
                } else
                    throw new IllegalArgumentException("Relation is not supported for string value: " + janusgraphPredicate);
            } else if (value instanceof Geoshape) {
                Preconditions.checkArgument(janusgraphPredicate instanceof Geo, "Relation not supported on geo types: " + janusgraphPredicate);
                Shape shape = ((Geoshape) value).getShape();
                SpatialOperation spatialOp = SPATIAL_PREDICATES.get((Geo) janusgraphPredicate);
                SpatialArgs args = new SpatialArgs(spatialOp, shape);
                params.addQuery(getSpatialStrategy(key, informations.get(key)).makeQuery(args));
            } else if (value instanceof Date) {
                Preconditions.checkArgument(janusgraphPredicate instanceof Cmp, "Relation not supported on date types: " + janusgraphPredicate);
                params.addQuery(numericQuery(key, (Cmp) janusgraphPredicate, ((Date) value).getTime()));
            } else if (value instanceof Instant) {
                Preconditions.checkArgument(janusgraphPredicate instanceof Cmp, "Relation not supported on instant types: " + janusgraphPredicate);
                params.addQuery(numericQuery(key, (Cmp) janusgraphPredicate, ((Instant) value).toEpochMilli()));
            }else if (value instanceof Boolean) {
                Preconditions.checkArgument(janusgraphPredicate instanceof Cmp, "Relation not supported on boolean types: " + janusgraphPredicate);
                int intValue;
                switch ((Cmp)janusgraphPredicate) {
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
                    BooleanQuery.Builder q = new BooleanQuery.Builder();
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
            SearchParams childParams = convertQuery(((Not) condition).getChild(), informations);
            params.addQuery(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
            params.addParams(childParams, BooleanClause.Occur.MUST_NOT);
        } else if (condition instanceof And) {
            for (Condition c : condition.getChildren()) {
                SearchParams childParams = convertQuery(c, informations);
                params.addParams(childParams, BooleanClause.Occur.MUST);
            }
        } else if (condition instanceof Or) {
            for (Condition c : condition.getChildren()) {
                SearchParams childParams = convertQuery(c, informations);
                params.addParams(childParams, BooleanClause.Occur.SHOULD);
            }
        } else throw new IllegalArgumentException("Invalid condition: " + condition);

        return params;
    }

    @Override
    public Stream<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        Query q;
        try {
            q = new QueryParser("_all",analyzer).parse(query.getQuery());
            // Lucene query parser does not take additional parameters so any parameters on the RawQuery are ignored. 
        } catch (ParseException e) {
            throw new PermanentBackendException("Could not parse raw query: "+query.getQuery(),e);
        }

        try {
            IndexSearcher searcher = ((Transaction) tx).getSearcher(query.getStore());
            if (searcher == null) return Collections.unmodifiableList(new ArrayList<RawQuery.Result<String>>()).stream(); //Index does not yet exist

            long time = System.currentTimeMillis();
            //TODO: can we make offset more efficient in Lucene?
            final int offset = query.getOffset();
            int adjustedLimit = query.hasLimit() ? query.getLimit() : Integer.MAX_VALUE - 1;
            if (adjustedLimit < Integer.MAX_VALUE-1-offset) adjustedLimit+=offset;
            else adjustedLimit = Integer.MAX_VALUE-1;
            final TopDocs docs = searcher.search(q, adjustedLimit);
            log.debug("Executed query [{}] in {} ms",q, System.currentTimeMillis() - time);
            List<RawQuery.Result<String>> result = new ArrayList<RawQuery.Result<String>>(docs.scoreDocs.length);
            for (int i = offset; i < docs.scoreDocs.length; i++) {
                final IndexableField field = searcher.doc(docs.scoreDocs[i].doc).getField(DOCID);
                result.add(new RawQuery.Result<String>(field == null ? null : field.stringValue(),docs.scoreDocs[i].score));
            }
            return result.stream();
        } catch (IOException e) {
            throw new TemporaryBackendException("Could not execute Lucene query", e);
        }
    }

    @Override
    public Long totals(RawQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        final Query q;
        try {
            q = new QueryParser("_all",analyzer).parse(query.getQuery());
        } catch (ParseException e) {
            throw new PermanentBackendException("Could not parse raw query: "+query.getQuery(),e);
        }

        try {
            final IndexSearcher searcher = ((Transaction) tx).getSearcher(query.getStore());
            if (searcher == null) return 0L; //Index does not yet exist

            final long time = System.currentTimeMillis();
            // Lucene doesn't like limits of 0.  Also, it doesn't efficiently build a total list.
            query.setLimit(1);
            // We ignore offset and limit for totals
            final TopDocs docs = searcher.search(q, 1);
            log.debug("Executed query [{}] in {} ms",q, System.currentTimeMillis() - time);
            return new Long(docs.totalHits);
        } catch (IOException e) {
            throw new TemporaryBackendException("Could not execute Lucene query", e);
        }
    }
    
    @Override
    public BaseTransactionConfigurable beginTransaction(BaseTransactionConfig config) throws BackendException {
        return new Transaction(config);
    }

    @Override
    public boolean supports(KeyInformation information, JanusGraphPredicate janusgraphPredicate) {
        if (information.getCardinality()!= Cardinality.SINGLE) return false;
        Class<?> dataType = information.getDataType();
        Mapping mapping = Mapping.getMapping(information);
        if (mapping!=Mapping.DEFAULT && !AttributeUtil.isString(dataType) &&
                !(mapping==Mapping.PREFIX_TREE && AttributeUtil.isGeo(dataType))) return false;

        if (Number.class.isAssignableFrom(dataType)) {
            if (janusgraphPredicate instanceof Cmp) return true;
        } else if (dataType == Geoshape.class) {
            return janusgraphPredicate == Geo.INTERSECT || janusgraphPredicate == Geo.WITHIN || janusgraphPredicate == Geo.CONTAINS;
        } else if (AttributeUtil.isString(dataType)) {
            switch(mapping) {
                case DEFAULT:
                case TEXT:
                    return janusgraphPredicate == Text.CONTAINS || janusgraphPredicate == Text.CONTAINS_PREFIX || janusgraphPredicate == Text.CONTAINS_FUZZY; // || janusgraphPredicate == Text.CONTAINS_REGEX;
                case STRING:
                    return janusgraphPredicate == Cmp.EQUAL || janusgraphPredicate==Cmp.NOT_EQUAL || janusgraphPredicate==Text.PREFIX || janusgraphPredicate==Text.REGEX  || janusgraphPredicate == Text.FUZZY;
            }
        } else if (dataType == Date.class || dataType == Instant.class) {
            if (janusgraphPredicate instanceof Cmp) return true;
        } else if (dataType == Boolean.class) {
            return janusgraphPredicate == Cmp.EQUAL || janusgraphPredicate == Cmp.NOT_EQUAL;
        } else if (dataType == UUID.class) {
            return janusgraphPredicate == Cmp.EQUAL || janusgraphPredicate == Cmp.NOT_EQUAL;
        }
        return false;
    }

    @Override
    public boolean supports(KeyInformation information) {
        if (information.getCardinality()!= Cardinality.SINGLE) return false;
        Class<?> dataType = information.getDataType();
        Mapping mapping = Mapping.getMapping(information);
        if (Number.class.isAssignableFrom(dataType) || dataType == Date.class || dataType == Instant.class || dataType == Boolean.class || dataType == UUID.class) {
            if (mapping==Mapping.DEFAULT) return true;
        } else if (AttributeUtil.isString(dataType)) {
            if (mapping==Mapping.DEFAULT || mapping==Mapping.STRING || mapping==Mapping.TEXT) return true;
        } else if (AttributeUtil.isGeo(dataType)) {
            if (mapping==Mapping.DEFAULT || mapping==Mapping.PREFIX_TREE) return true;
        }
        return false;
    }

    @Override
    public String mapKey2Field(String key, KeyInformation information) {
        Preconditions.checkArgument(!StringUtils.containsAny(key,new char[]{' '}),"Invalid key name provided: %s",key);
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
        } catch (IOException e) {
            throw new PermanentBackendException("Could not delete lucene directory: " + basePath, e);
        }
    }

    @Override
    public boolean exists() throws BackendException {
        if (Files.exists(Paths.get(basePath))) {
            try (final DirectoryStream<Path> dirStream = Files.newDirectoryStream(Paths.get(basePath))) {
                return dirStream.iterator().hasNext();
            } catch (IOException e) {
                throw new PermanentBackendException("Could not read lucene directory: " + basePath, e);
            }
        } else {
            return false;
        }
    }

    private class Transaction implements BaseTransactionConfigurable {

        private final BaseTransactionConfig config;
        private final Set<String> updatedStores = Sets.newHashSet();
        private final Map<String, IndexSearcher> searchers = new HashMap<String, IndexSearcher>(4);

        private Transaction(BaseTransactionConfig config) {
            this.config = config;
        }

        private synchronized IndexSearcher getSearcher(String store) throws BackendException {
            IndexSearcher searcher = searchers.get(store);
            if (searcher == null) {
                IndexReader reader = null;
                try {
                    reader = DirectoryReader.open(getStoreDirectory(store));
                    searcher = new IndexSearcher(reader);
                } catch (IndexNotFoundException e) {
                    searcher = null;
                } catch (IOException e) {
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
                for (IndexSearcher searcher : searchers.values()) {
                    if (searcher != null) searcher.getIndexReader().close();
                }
            } catch (IOException e) {
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
        private BooleanQuery.Builder qb = new BooleanQuery.Builder();

        private void addQuery(Query newQuery) {
            addQuery(newQuery, BooleanClause.Occur.MUST);
        }

        private void addQuery(Query newQuery, BooleanClause.Occur occur) {
            qb.add(newQuery, occur);
        }

        private void addParams(SearchParams other, BooleanClause.Occur occur) {
            Query otherQuery = other.getQuery();
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
}
