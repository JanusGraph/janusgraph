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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public enum ElasticMajorVersion {

    ONE(1),

    TWO(2),

    FIVE(5),

    SIX(6),

    ;

    static final Pattern PATTERN = Pattern.compile("(\\d+)\\.\\d+\\.\\d+.*");

    int value;

    ElasticMajorVersion(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static ElasticMajorVersion parse(final String value) {
        final Matcher m = value != null ? PATTERN.matcher(value) : null;
        switch (m != null && m.find() ? Integer.valueOf(m.group(1)) : -1) {
            case 1:
                return ElasticMajorVersion.ONE;
            case 2:
                return ElasticMajorVersion.TWO;
            case 5:
                return ElasticMajorVersion.FIVE;
            case 6:
                return ElasticMajorVersion.SIX;
            default:
                throw new IllegalArgumentException("Unsupported Elasticsearch server major version: " + value);
        }
    }
}
