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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ElasticMajorVersionTest {

    private static Map<String, Object> serverVersion(String distribution, String number) {
        final Map<String, Object> version = new HashMap<>();
        if (distribution != null) {
            version.put("distribution", distribution);
        }
        version.put("number", number);
        return version;
    }

    @Test
    public void testElasticsearchVersions() {
        assertEquals(ElasticMajorVersion.SIX, ElasticMajorVersion.fromServerVersion(serverVersion(null, "6.6.0")));
        assertEquals(ElasticMajorVersion.SEVEN, ElasticMajorVersion.fromServerVersion(serverVersion(null, "7.17.8")));
        assertEquals(ElasticMajorVersion.EIGHT, ElasticMajorVersion.fromServerVersion(serverVersion(null, "8.15.3")));
        assertEquals(ElasticMajorVersion.NINE, ElasticMajorVersion.fromServerVersion(serverVersion(null, "9.5.4")));
    }

    @Test
    public void testOpenSearchProvidesTheElasticsearch7Api() {
        assertEquals(ElasticMajorVersion.SEVEN, ElasticMajorVersion.fromServerVersion(serverVersion("opensearch", "2.19.6")));
        assertEquals(ElasticMajorVersion.SEVEN, ElasticMajorVersion.fromServerVersion(serverVersion("opensearch", "3.9.0")));
        // OpenSearch 2 reports 7.10.2 without a distribution if compatibility.override_main_response_version is set
        assertEquals(ElasticMajorVersion.SEVEN, ElasticMajorVersion.fromServerVersion(serverVersion(null, "7.10.2")));
    }

    @Test
    public void testIsOpenSearch() {
        assertTrue(ElasticMajorVersion.isOpenSearch(serverVersion("opensearch", "3.9.0")));
        assertFalse(ElasticMajorVersion.isOpenSearch(serverVersion(null, "7.17.8")));
        assertFalse(ElasticMajorVersion.isOpenSearch(serverVersion(null, "7.10.2")));
        assertFalse(ElasticMajorVersion.isOpenSearch(null));
    }

    @Test
    public void testUnsupportedVersions() {
        assertThrows(IllegalArgumentException.class, () -> ElasticMajorVersion.fromServerVersion(serverVersion(null, "5.6.16")));
        assertThrows(IllegalArgumentException.class, () -> ElasticMajorVersion.fromServerVersion(serverVersion(null, "10.0.0")));
        assertThrows(IllegalArgumentException.class, () -> ElasticMajorVersion.fromServerVersion(null));
    }

    @Test
    public void testUnsupportedOpenSearchVersions() {
        // OpenSearch 1 reached its end of life
        assertThrows(IllegalArgumentException.class, () -> ElasticMajorVersion.fromServerVersion(serverVersion("opensearch", "1.3.20")));
        assertThrows(IllegalArgumentException.class, () -> ElasticMajorVersion.fromServerVersion(serverVersion("opensearch", "4.0.0")));
        assertThrows(IllegalArgumentException.class, () -> ElasticMajorVersion.fromServerVersion(serverVersion("opensearch", "10.1.0")));
        assertThrows(IllegalArgumentException.class, () -> ElasticMajorVersion.fromServerVersion(serverVersion("opensearch", "next")));
        assertThrows(IllegalArgumentException.class, () -> ElasticMajorVersion.fromServerVersion(serverVersion("opensearch", null)));
    }

    @Test
    public void testOf() {
        assertEquals(ElasticMajorVersion.SEVEN, ElasticMajorVersion.of(7));
        assertEquals(ElasticMajorVersion.NINE, ElasticMajorVersion.of(9));
        assertThrows(IllegalArgumentException.class, () -> ElasticMajorVersion.of(5));
    }
}
