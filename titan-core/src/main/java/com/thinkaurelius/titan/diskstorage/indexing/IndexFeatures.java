package com.thinkaurelius.titan.diskstorage.indexing;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.schema.Mapping;

import java.util.Arrays;
import java.util.Set;

/**
 * Characterizes the features that a particular {@link IndexProvider} implementation supports
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IndexFeatures {

    private final boolean supportsDocumentTTL;
    private final Mapping defaultStringMapping;
    private final ImmutableSet<Mapping> supportedStringMappings;
    private final String wildcardField;
    private final boolean supportsNanoseconds;
    private ImmutableSet<Cardinality> supportedCardinaities;

    public IndexFeatures(boolean supportsDocumentTTL,
                         Mapping defaultMap,
                         ImmutableSet<Mapping> supportedMap, String wildcardField, ImmutableSet<Cardinality> supportedCardinaities, boolean supportsNanoseconds) {

        Preconditions.checkArgument(defaultMap!=null || defaultMap!=Mapping.DEFAULT);
        Preconditions.checkArgument(supportedMap!=null && !supportedMap.isEmpty()
                                    && supportedMap.contains(defaultMap));
        this.supportsDocumentTTL = supportsDocumentTTL;
        this.defaultStringMapping = defaultMap;
        this.supportedStringMappings = supportedMap;
        this.wildcardField = wildcardField;
        this.supportedCardinaities = supportedCardinaities;
        this.supportsNanoseconds = supportsNanoseconds;
    }

    public boolean supportsDocumentTTL() {
        return supportsDocumentTTL;
    }

    public Mapping getDefaultStringMapping() {
        return defaultStringMapping;
    }

    public boolean supportsStringMapping(Mapping map) {
        return supportedStringMappings.contains(map);
    }

    public String getWildcardField() {
        return wildcardField;
    }

    public boolean supportsCardinality(Cardinality cardinality) {
        return supportedCardinaities.contains(cardinality);
    }

    public boolean supportsNanoseconds() {
        return supportsNanoseconds;
    }

    public static class Builder {

        private boolean supportsDocumentTTL = false;
        private Mapping defaultStringMapping = Mapping.TEXT;
        private Set<Mapping> supportedMappings = Sets.newHashSet();
        private Set<Cardinality> supportedCardinalities = Sets.newHashSet();
        private String wildcardField = "*";
        private boolean supportsNanoseconds;

        public Builder supportsDocumentTTL() {
            supportsDocumentTTL=true;
            return this;
        }

        public Builder setDefaultStringMapping(Mapping map) {
            defaultStringMapping = map;
            return this;
        }

        public Builder supportedStringMappings(Mapping... maps) {
            supportedMappings.addAll(Arrays.asList(maps));
            return this;
        }

        public Builder supportsCardinality(Cardinality cardinality) {
            supportedCardinalities.add(cardinality);
            return this;
        }

        public Builder setWildcardField(String wildcardField) {
            this.wildcardField = wildcardField;
            return this;
        }

        public Builder supportsNanoseconds() {
            supportsNanoseconds = true;
            return this;
        }

        public IndexFeatures build() {
            return new IndexFeatures(supportsDocumentTTL, defaultStringMapping,
                    ImmutableSet.copyOf(supportedMappings), wildcardField,  ImmutableSet.copyOf(supportedCardinalities), supportsNanoseconds);
        }


    }

}
