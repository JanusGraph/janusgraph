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

package org.janusgraph.diskstorage.es.compat;

import com.google.common.collect.ImmutableMap;
import org.janusgraph.diskstorage.indexing.IndexFeatures;

import java.util.Map;

import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_INDEX_KEY;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_TYPE_KEY;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_ANALYZER;

/**
 * Mapping and query object builder for Elasticsearch 2.x.
 */
public class ES2Compat extends AbstractESCompat {

    private static final IndexFeatures FEATURES = coreFeatures().supportsGeoContains().build();

    private static final String STRING_TYPE_NAME = "string";

    @Override
    public String scriptLang() {
        return "groovy";
    }

    @Override
    public Map<String,Object> createKeywordMapping() {
        return ImmutableMap.of(ES_TYPE_KEY, STRING_TYPE_NAME, ES_INDEX_KEY, "not_analyzed");
    }

    @Override
    public Map<String,Object> createTextMapping(String textAnalyzer) {
        final ImmutableMap.Builder builder = ImmutableMap.builder().put(ES_TYPE_KEY, STRING_TYPE_NAME);
        return (textAnalyzer != null ? builder.put(ES_ANALYZER, textAnalyzer) : builder).build();
    }

    @Override
    public IndexFeatures getIndexFeatures() {
        return FEATURES;
    }

}
