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

package org.janusgraph.diskstorage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This enum is only intended for use by JanusGraph internals.
 * It is subject to backwards-incompatible change.
 */
public enum StandardIndexProvider {
    LUCENE("org.janusgraph.diskstorage.lucene.LuceneIndex", "lucene"),
    ELASTICSEARCH("org.janusgraph.diskstorage.es.ElasticSearchIndex", ImmutableList.of("elasticsearch", "es")),
    SOLR("org.janusgraph.diskstorage.solr.SolrIndex", "solr");

    private final String providerName;
    private final ImmutableList<String> shorthands;

    StandardIndexProvider(String providerName, String shorthand) {
        this(providerName, ImmutableList.of(shorthand));
    }

    StandardIndexProvider(String providerName, ImmutableList<String> shorthands) {
        this.providerName = providerName;
        this.shorthands = shorthands;
    }

    private static final ImmutableList<String> ALL_SHORTHANDS;
    private static final ImmutableMap<String, String> ALL_MANAGER_CLASSES;

    public List<String> getShorthands() {
        return shorthands;
    }

    public String getProviderName() {
        return providerName;
    }

    static {
        StandardIndexProvider[] backends = values();
        final List<String> tempShorthands = new ArrayList<>();
        final Map<String, String> tempClassMap = new HashMap<>();
        for (final StandardIndexProvider backend : backends) {
            tempShorthands.addAll(backend.getShorthands());
            for (final String shorthand : backend.getShorthands()) {
                tempClassMap.put(shorthand, backend.getProviderName());
            }
        }
        ALL_SHORTHANDS = ImmutableList.copyOf(tempShorthands);
        ALL_MANAGER_CLASSES = ImmutableMap.copyOf(tempClassMap);
    }

    public static List<String> getAllShorthands() {
        return ALL_SHORTHANDS;
    }

    public static Map<String, String> getAllProviderClasses() {
        return ALL_MANAGER_CLASSES;
    }
}
