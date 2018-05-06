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

package org.janusgraph.diskstorage.indexing;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.schema.Mapping;

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
    private final boolean supportsCustomAnalyzer;
    private final boolean supportsGeoContains;
    private final boolean supportsNotQueryNormalForm;
    private final ImmutableSet<Cardinality> supportedCardinalities;

    public IndexFeatures(boolean supportsDocumentTTL, Mapping defaultMap, ImmutableSet<Mapping> supportedMap,
                         String wildcardField, ImmutableSet<Cardinality> supportedCardinalities, boolean supportsNanoseconds,
                         boolean supportCustomAnalyzer, boolean supportsGeoContains, boolean supportsNotQueryNormalForm) {

        Preconditions.checkArgument(defaultMap!=null && defaultMap!=Mapping.DEFAULT);
        Preconditions.checkArgument(supportedMap!=null && !supportedMap.isEmpty()
                                    && supportedMap.contains(defaultMap));
        this.supportsDocumentTTL = supportsDocumentTTL;
        this.defaultStringMapping = defaultMap;
        this.supportedStringMappings = supportedMap;
        this.wildcardField = wildcardField;
        this.supportedCardinalities = supportedCardinalities;
        this.supportsNanoseconds = supportsNanoseconds;
        this.supportsCustomAnalyzer = supportCustomAnalyzer;
        this.supportsGeoContains = supportsGeoContains;
        this.supportsNotQueryNormalForm = supportsNotQueryNormalForm;
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
        return supportedCardinalities.contains(cardinality);
    }

    public boolean supportsNanoseconds() {
        return supportsNanoseconds;
    }

    public boolean supportsCustomAnalyzer() {
        return supportsCustomAnalyzer;
    }

    public boolean supportsGeoContains() {
        return supportsGeoContains;
    }

    public boolean supportNotQueryNormalForm() {
        return supportsNotQueryNormalForm;
    }

    public static class Builder {

        private boolean supportsDocumentTTL = false;
        private Mapping defaultStringMapping = Mapping.TEXT;
        private final Set<Mapping> supportedMappings = Sets.newHashSet();
        private final Set<Cardinality> supportedCardinalities = Sets.newHashSet();
        private String wildcardField = "*";
        private boolean supportsNanoseconds;
        private boolean supportsCustomAnalyzer;
        private boolean supportsGeoContains;
        private boolean supportNotQueryNormalForm;

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

        public Builder supportsCustomAnalyzer() {
            supportsCustomAnalyzer = true;
            return this;
        }

        public Builder supportsGeoContains() {
            this.supportsGeoContains = true;
            return this;
        }

        public Builder supportNotQueryNormalForm() {
            this.supportNotQueryNormalForm = true;
            return this;
        }

        public IndexFeatures build() {
            return new IndexFeatures(supportsDocumentTTL, defaultStringMapping, ImmutableSet.copyOf(supportedMappings),
                wildcardField,  ImmutableSet.copyOf(supportedCardinalities), supportsNanoseconds, supportsCustomAnalyzer,
                supportsGeoContains, supportNotQueryNormalForm);
        }
    }
}
