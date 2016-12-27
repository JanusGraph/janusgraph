package com.thinkaurelius.titan.diskstorage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This enum is only intended for use by Titan internals.
 * It is subject to backwards-incompatible change.
 */
public enum StandardIndexProvider {
    LUCENE("com.thinkaurelius.titan.diskstorage.lucene.LuceneIndex", "lucene"),
    ELASTICSEARCH("com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex", ImmutableList.of("elasticsearch", "es")),
    SOLR("com.thinkaurelius.titan.diskstorage.solr.SolrIndex", "solr");

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
        StandardIndexProvider backends[] = values();
        List<String> tempShorthands = new ArrayList<String>();
        Map<String, String> tempClassMap = new HashMap<String, String>();
        for (int i = 0; i < backends.length; i++) {
            tempShorthands.addAll(backends[i].getShorthands());
            for (String shorthand : backends[i].getShorthands()) {
                tempClassMap.put(shorthand, backends[i].getProviderName());
            }
        }
        ALL_SHORTHANDS = ImmutableList.copyOf(tempShorthands);
        ALL_MANAGER_CLASSES = ImmutableMap.copyOf(tempClassMap);
    }

    public static final List<String> getAllShorthands() {
        return ALL_SHORTHANDS;
    }

    public static final Map<String, String> getAllProviderClasses() {
        return ALL_MANAGER_CLASSES;
    }
}
