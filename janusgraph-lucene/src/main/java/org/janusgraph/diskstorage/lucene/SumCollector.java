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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;

import java.io.IOException;
import java.util.Set;

public class SumCollector extends SimpleCollector {
    double sum = 0.0;
    final String fieldName;
    LeafReaderContext context;
    final IndexSearcher searcher;
    final Set<String> fieldSet;

    public SumCollector(String fieldName, IndexSearcher searcher) {
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
        sum += searcher.doc(context.docBase + doc, fieldSet).getField(fieldName).numericValue().doubleValue();
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    public double getValue() {
        return sum;
    }
}
