// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.diskstorage.es;

import org.janusgraph.graphdb.configuration.ConfigName;

/**
 * How a result larger than a page is read, through {@link ElasticSearchIndex#PAGING_MODE}. A point in time is used
 * only where the cluster has one, see {@link ElasticSearchClient#supportsPointInTime()}; every other cluster scrolls.
 */
public enum ElasticSearchPagingMode implements ConfigName {

    /**
     * Every result is read through a scroll.
     */
    SCROLL("scroll"),

    /**
     * Every result is read through a point in time and {@code search_after}.
     */
    POINT_IN_TIME("point_in_time"),

    /**
     * A result is read through a point in time where that is about as fast as a scroll or faster: where its pages come
     * in the order of the index, those of a graph query without a limit and without a sort, on an index of at most
     * {@link ElasticSearchIndex#ADAPTIVE_POINT_IN_TIME_MAX_SHARDS} shards and with pages of at most
     * {@link ElasticSearchIndex#ADAPTIVE_POINT_IN_TIME_MAX_PAGE_SIZE} hits. Every other result is read through a
     * scroll, which reads pages in another order, through more shards or larger ones faster.
     */
    ADAPTIVE_POINT_IN_TIME("adaptive_point_in_time");

    private final String configName;

    ElasticSearchPagingMode(String configName) {
        this.configName = configName;
    }

    @Override
    public String getConfigName() {
        return configName;
    }

    /**
     * @return the mode of the given configuration value, or null where it names none
     */
    public static ElasticSearchPagingMode fromConfigName(String configName) {
        for (final ElasticSearchPagingMode mode : values()) {
            if (mode.configName.equals(configName)) {
                return mode;
            }
        }
        return null;
    }
}
