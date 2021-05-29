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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * This enum is only intended for use by JanusGraph internals.
 * It is subject to backwards-incompatible change.
 */
public enum StandardIndexProvider {
    LUCENE("org.janusgraph.diskstorage.lucene.LuceneIndex", "lucene"),
    ELASTICSEARCH("org.janusgraph.diskstorage.es.ElasticSearchIndex", Arrays.asList("elasticsearch", "es")),
    SOLR("org.janusgraph.diskstorage.solr.SolrIndex", "solr");

    private static final Set<String> ALL_SHORTHANDS;
    private static final Map<String, String> ALL_MANAGER_CLASSES;

    private final String providerName;
    private final Set<String> shorthands;

    StandardIndexProvider(String providerName, String shorthand) {
        this(providerName, Collections.singleton(shorthand));
    }

    StandardIndexProvider(String providerName, Collection<String> shorthands) {
        this.providerName = providerName;
        this.shorthands = Collections.unmodifiableSet(new LinkedHashSet<>(shorthands));
    }

    public Set<String> getShorthands() {
        return shorthands;
    }

    public String getProviderName() {
        return providerName;
    }

    static {
        StandardIndexProvider[] backends = values();
        final Set<String> tempShorthands = new LinkedHashSet<>(backends.length);
        final Map<String, String> tempClassMap = new HashMap<>(backends.length);
        for (final StandardIndexProvider backend : backends) {
            backend.getShorthands().forEach(shorthand -> {
                tempShorthands.add(shorthand);
                tempClassMap.put(shorthand, backend.getProviderName());
            });
        }
        ALL_SHORTHANDS = Collections.unmodifiableSet(tempShorthands);
        ALL_MANAGER_CLASSES = Collections.unmodifiableMap(tempClassMap);
    }

    public static Set<String> getAllShorthands() {
        return ALL_SHORTHANDS;
    }

    public static Map<String, String> getAllProviderClasses() {
        return ALL_MANAGER_CLASSES;
    }
}
