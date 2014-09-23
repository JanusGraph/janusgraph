package com.thinkaurelius.titan.diskstorage.indexing;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
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
    private final Mapping defaultMapping;
    private final ImmutableSet<Mapping> supportedMappings;

    public IndexFeatures(boolean supportsDocumentTTL,
                         Mapping defaultMap,
                         ImmutableSet<Mapping> supportedMap) {
        Preconditions.checkArgument(defaultMap!=null || defaultMap!=Mapping.DEFAULT);
        Preconditions.checkArgument(supportedMap!=null && !supportedMap.isEmpty()
                                    && supportedMap.contains(defaultMap));
        this.supportsDocumentTTL = supportsDocumentTTL;
        this.defaultMapping = defaultMap;
        this.supportedMappings = supportedMap;
    }

    public boolean supportsDocumentTTL() {
        return supportsDocumentTTL;
    }

    public Mapping getDefaultStringMapping() {
        return defaultMapping;
    }

    public boolean supportsStringMapping(Mapping map) {
        return supportedMappings.contains(map);
    }

    public static class Builder {

        private boolean supportsDocumentTTL = false;
        private Mapping defaultMapping = Mapping.TEXT;
        private Set<Mapping> supportedMappings = Sets.newHashSet();


        public Builder supportsDocumentTTL() {
            supportsDocumentTTL=true;
            return this;
        }

        public Builder setDefaultMapping(Mapping map) {
            defaultMapping = map;
            return this;
        }

        public Builder supportedMappings(Mapping... maps) {
            supportedMappings.addAll(Arrays.asList(maps));
            return this;
        }


        public IndexFeatures build() {
            return new IndexFeatures(supportsDocumentTTL,defaultMapping,
                    ImmutableSet.copyOf(supportedMappings));
        }


    }

}
