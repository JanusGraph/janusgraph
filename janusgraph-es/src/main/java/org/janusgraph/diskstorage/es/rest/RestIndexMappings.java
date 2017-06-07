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

package org.janusgraph.diskstorage.es.rest;

import java.util.Map;

/**
 * Serialization of Elasticsearch index mapping.
 *
 * @author David Clement (davidclement90@laposte.net)
 */
public class RestIndexMappings {

    private Map<String, RestIndexMapping> mappings;

    public Map<String, RestIndexMapping> getMappings() {
        return mappings;
    }

    public void setMappings(Map<String, RestIndexMapping> mappings){
        this.mappings = mappings;
    }

    public static class RestIndexMapping {

        private Map<String, Object> properties;

        public Map<String, Object> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, Object> properties) {
            this.properties = properties;
        }
    }
}
