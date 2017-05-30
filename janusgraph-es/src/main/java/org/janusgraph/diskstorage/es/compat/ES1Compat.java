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

import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_LANG_KEY;
import static org.janusgraph.diskstorage.es.ElasticSearchConstants.ES_SCRIPT_KEY;

/**
 * Mapping and query object builder for Elasticsearch 1.x.
 */
public class ES1Compat extends ES2Compat {

    private static final IndexFeatures FEATURES = coreFeatures().build();

    @Override
    public IndexFeatures getIndexFeatures() {
        return FEATURES;
    }

    @Override
    public ImmutableMap.Builder prepareScript(String script) {
        return ImmutableMap.builder().put(ES_SCRIPT_KEY, ImmutableMap.of(ES_SCRIPT_KEY, script, ES_LANG_KEY, scriptLang()));
    }

    @Override
    public Map<String,Object> prepareQuery(Map<String,Object> query) {
        return ImmutableMap.of("filtered", ImmutableMap.of("filter", query));
    }

    @Override
    public Map<String,Object> match(String key, Object value, String fuzziness) {
        return ImmutableMap.of("query", super.match(key, value, fuzziness));
    }

    @Override
    public Map<String,Object> filter(Map<String,Object> query) {
        return query;
    }

}
