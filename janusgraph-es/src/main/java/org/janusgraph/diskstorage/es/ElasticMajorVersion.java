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

package org.janusgraph.diskstorage.es;

import com.google.common.base.Predicate;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public enum ElasticMajorVersion {

    SIX(6),

    SEVEN(7),

    EIGHT(8),

    NINE(9),
    ;

    static final Pattern PATTERN = Pattern.compile("(\\d+)\\.\\d+\\.\\d+.*");

    public static final String OPENSEARCH_DISTRIBUTION = "opensearch";

    //The OpenSearch major versions which JanusGraph supports. They provide the Elasticsearch 7 API without mapping
    //types. OpenSearch 1, which still had mapping types, reached its end of life in May 2025.
    private static final int MIN_OPENSEARCH_MAJOR_VERSION = 2;
    private static final int MAX_OPENSEARCH_MAJOR_VERSION = 3;

    final int value;

    ElasticMajorVersion(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static ElasticMajorVersion parse(final String value) {
        final Matcher m = value != null ? PATTERN.matcher(value) : null;
        final ElasticMajorVersion version = m != null && m.find() ? find(Integer.parseInt(m.group(1))) : null;
        if (version == null) {
            throw new IllegalArgumentException("Unsupported Elasticsearch server major version: " + value);
        }
        return version;
    }

    /**
     * Returns the major version with the given number, for example {@link #SEVEN} for 7.
     */
    public static ElasticMajorVersion of(final int value) {
        final ElasticMajorVersion version = find(value);
        if (version == null) {
            throw new IllegalArgumentException("Unsupported Elasticsearch server major version: " + value);
        }
        return version;
    }

    /**
     * Returns the major version of the Elasticsearch API which a cluster provides, given the {@code version} object of
     * its root endpoint. OpenSearch reports its own version with the distribution {@code opensearch}, and OpenSearch 2
     * and 3 provide the Elasticsearch 7 API it was forked from. Other OpenSearch versions are rejected like unsupported
     * Elasticsearch versions.
     */
    public static ElasticMajorVersion fromServerVersion(final Map<String, Object> version) {
        if (isOpenSearch(version)) {
            final int openSearchMajorVersion = majorNumber(version.get("number"));
            if (openSearchMajorVersion < MIN_OPENSEARCH_MAJOR_VERSION || openSearchMajorVersion > MAX_OPENSEARCH_MAJOR_VERSION) {
                throw new IllegalArgumentException("Unsupported OpenSearch server major version: " + version.get("number"));
            }
            return SEVEN;
        }
        return parse(version != null ? (String) version.get("number") : null);
    }

    /**
     * Returns whether the {@code version} object of a cluster's root endpoint belongs to OpenSearch.
     */
    public static boolean isOpenSearch(final Map<String, Object> version) {
        return version != null && OPENSEARCH_DISTRIBUTION.equals(version.get("distribution"));
    }

    //The major number of a version like 2.19.6, or -1 if it isn't one
    private static int majorNumber(final Object number) {
        final Matcher m = number != null ? PATTERN.matcher(number.toString()) : null;
        return m != null && m.find() ? Integer.parseInt(m.group(1)) : -1;
    }

    /**
     * Returns a predicate which accepts the numbers of the supported major versions.
     */
    public static Predicate<Integer> supportedNumbers() {
        return value -> value != null && find(value) != null;
    }

    private static ElasticMajorVersion find(final int value) {
        for (final ElasticMajorVersion version : values()) {
            if (version.value == value) {
                return version;
            }
        }
        return null;
    }
}
