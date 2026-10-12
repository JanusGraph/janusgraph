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

import com.google.common.collect.Sets;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;

import java.io.IOException;
import java.util.Set;

/**
 * Collects the number, sum, minimum and maximum of the stored values of a numeric field in the matching documents, each
 * of which may hold several values of the field or none
 */
public class StatsCollector extends SimpleCollector {
    final String fieldName;
    LeafReaderContext context;
    final IndexSearcher searcher;
    final Set<String> fieldSet;
    private long count = 0;
    private double sum = 0.0;
    private Number min;
    private Number max;

    public StatsCollector(String fieldName, IndexSearcher searcher) {
        this.fieldName = fieldName;
        this.searcher = searcher;
        this.fieldSet = Sets.newHashSet(fieldName);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
        this.context = context;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {}

    @Override
    public void collect(int doc) throws IOException {
        for (final IndexableField field : searcher.doc(context.docBase + doc, fieldSet).getFields(fieldName)) {
            final Number value = field.numericValue();
            count++;
            sum += value.doubleValue();
            if (min == null || compare(value, min) < 0) min = value;
            if (max == null || compare(value, max) > 0) max = value;
        }
    }

    // Whole numbers are stored as longs, which doubles can't all tell apart, and decimals as doubles
    private static int compare(Number a, Number b) {
        return a instanceof Long ? Long.compare(a.longValue(), b.longValue()) : Double.compare(a.doubleValue(), b.doubleValue());
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    public long getCount() {
        return count;
    }

    public double getSum() {
        return sum;
    }

    // null when there are no values
    public Number getMin() {
        return min;
    }

    // null when there are no values
    public Number getMax() {
        return max;
    }
}
