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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.util.system.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Shape;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class LuceneExample {

    public static final File path = new File("/tmp/lucene");
    private static final String STR_SUFFIX = "_str";
    private static final String TXT_SUFFIX = "_txt";

    private static final int MAX_RESULT = 10000;

    private final Map<String,SpatialStrategy> spatial= new HashMap<>();
    private final SpatialContext ctx = SpatialContext.GEO;

    @BeforeEach
    public void setup() {
        if (path.exists()) IOUtils.deleteDirectory(path,false);
        if (!path.exists() && path.isDirectory()) {
            Preconditions.checkState(path.mkdirs());
        }
    }

    private SpatialStrategy getSpatialStrategy(String key) {
        SpatialStrategy strategy = spatial.get(key);
        if (strategy==null) {
            final int maxLevels = 11;
            SpatialPrefixTree grid = new GeohashPrefixTree(ctx, maxLevels);
            strategy = new RecursivePrefixTreeStrategy(grid, key);
            spatial.put(key,strategy);
        }
        return strategy;
    }

    @Test
    public void example1() throws Exception {
        Directory dir = FSDirectory.open(path.toPath());
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        IndexWriter writer = new IndexWriter(dir, iwc);

        indexDocs(writer, "doc1", ImmutableMap.of("name","The laborious work of John Doe as we know it",
                                                  "city","Blumenkamp",
                                                  "location",Geoshape.point(51.687882,6.612053),
                                                  "time",1000342034
                ));

        indexDocs(writer, "doc2", ImmutableMap.of("name","Life as we know it or not",
                "city","Essen",
                "location",Geoshape.point(51.787882,6.712053),
                "time",1000342034-500
        ));

        indexDocs(writer, "doc3", ImmutableMap.of("name","Berlin - poor but sexy and a display of the extraordinary",
                "city","Berlin",
                "location",Geoshape.circle(52.509535, 13.425293, 50),
                "time",1000342034+2000
        ));
        writer.close();

        //Search
        IndexReader reader = DirectoryReader.open(FSDirectory.open(path.toPath()));
        IndexSearcher searcher = new IndexSearcher(reader);

        //Auesee
        BooleanQuery.Builder filter = new BooleanQuery.Builder();

        SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects,Geoshape.circle(51.666167,6.58905,450).getShape());

        filter.add(LongPoint.newRangeQuery("time", 1000342034, 1000342034), BooleanClause.Occur.MUST);


        filter.add(new PrefixQuery(new Term("city_str","B")), BooleanClause.Occur.MUST);


        BooleanQuery.Builder qb = new BooleanQuery.Builder();
        qb.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
        qb.add(filter.build(), BooleanClause.Occur.FILTER);
        TopDocs docs = searcher.search(qb.build(), MAX_RESULT);
        if (docs.totalHits.value>=MAX_RESULT) throw new RuntimeException("Max results exceeded: " + MAX_RESULT);

        Set<String> result = getResults(searcher,docs);
        System.out.println(result);

    }

    private Set<String> getResults(IndexSearcher searcher, TopDocs docs) throws IOException {
        Set<String> found = Sets.newHashSet();
        for (int i = 0; i < docs.totalHits.value; i++) {
            found.add(searcher.doc(docs.scoreDocs[i].doc).getField("docid").stringValue());
        }
        return found;
    }

    void indexDocs(IndexWriter writer, String documentId, Map<String,Object> docMap) throws IOException {
        Document doc = new Document();

        Field documentIdField = new StringField("docid", documentId, Field.Store.YES);
        doc.add(documentIdField);

        for (Map.Entry<String,Object> kv : docMap.entrySet()) {
            String key = kv.getKey();
            Object value = kv.getValue();

            if (value instanceof Number) {
                final Field field;
                if (value instanceof Integer || value instanceof Long) {
                    field = new LongPoint(key, ((Number)value).longValue());
                } else { //double or float
                    field = new DoublePoint(key, ((Number)value).doubleValue());
                }
                doc.add(field);
            } else if (value instanceof String) {
                String str = (String)value;
                Field field = new TextField(key+ TXT_SUFFIX, str, Field.Store.NO);
                doc.add(field);
                if (str.length()<256)
                field = new StringField(key+STR_SUFFIX, str, Field.Store.NO);
                doc.add(field);
            } else if (value instanceof Geoshape) {
                Shape shape = ((Geoshape)value).getShape();
                for (IndexableField f : getSpatialStrategy(key).createIndexableFields(shape)) {
                    doc.add(f);
                }
            } else throw new IllegalArgumentException("Unsupported type: " + value);
        }

        writer.updateDocument(new Term("docid", documentId), doc);

    }


}
