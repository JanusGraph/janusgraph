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

import java.util.Map;

import org.apache.tinkerpop.shaded.jackson.annotation.JsonIgnoreProperties;

/**
 * Serialization of Elasticsearch index mapping.
 *
 * @author David Clement (davidclement90@laposte.net)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IndexMappings {

    private Map<String, IndexMapping> mappings;

    public Map<String, IndexMapping> getMappings() {
        return mappings;
    }

    public void setMappings(Map<String, IndexMapping> mappings){
        this.mappings = mappings;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class IndexMapping {

        private Map<String, Object> properties;

        private String dynamic;

        public Map<String, Object> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, Object> properties) {
            this.properties = properties;
        }

        public boolean isDynamic() {
            return dynamic == null || "true".equalsIgnoreCase(dynamic);
        }

        public void setDynamic(String dynamic) {
            this.dynamic = dynamic;
        }
    }
}
