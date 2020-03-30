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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.es.rest.RestClientSetup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Create an ES {@link org.elasticsearch.client.RestClient} from a JanusGraph
 * {@link org.janusgraph.diskstorage.configuration.Configuration}.
 * <p>
 * Any key-value pairs under the {@link org.janusgraph.diskstorage.es.ElasticSearchIndex#ES_CREATE_EXTRAS_NS} namespace
 * are copied into the Elasticsearch settings builder.  This allows overriding arbitrary
 * ES settings from within the JanusGraph properties file.
 * <p>
 * Assumes that an ES cluster is already running.  It does not attempt to start an
 * embedded ES instance.  It just connects to whatever hosts are given in
 * {@link org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration#INDEX_HOSTS}.
 */
public enum ElasticSearchSetup {

    /**
     * Create an ES RestClient connected to
     * {@link org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration#INDEX_HOSTS}.
     */
    REST_CLIENT {
        @Override
        public Connection connect(Configuration config) throws IOException {
            return new Connection(new RestClientSetup().connect(config));
        }
    };

    static Map<String, Object> getSettingsFromJanusGraphConf(Configuration config) {

        final Map<String, Object> settings = new HashMap<>();

        int keysLoaded = 0;
        final Map<String,Object> configSub = config.getSubset(ElasticSearchIndex.ES_CREATE_EXTRAS_NS);
        for (Map.Entry<String,Object> entry : configSub.entrySet()) {
            String key = entry.getKey();
            Object val = entry.getValue();
            if (null == val) continue;
            if (List.class.isAssignableFrom(val.getClass())) {
                // Pretty print lists using comma-separated values and no surrounding square braces for ES
                List l = (List) val;
                settings.put(key, Joiner.on(",").join(l));
            } else if (val.getClass().isArray()) {
                // As with Lists, but now for arrays
                // The Object copy[] business lets us avoid repetitive primitive array type checking and casting
                Object[] copy = new Object[Array.getLength(val)];
                for (int i= 0; i < copy.length; i++) {
                    copy[i] = Array.get(val, i);
                }
                settings.put(key, Joiner.on(",").join(copy));
            } else {
                // Copy anything else unmodified
                settings.put(key, val.toString());
            }
            log.debug("[ES ext.* cfg] Set {}: {}", key, val);
            keysLoaded++;
        }
        log.debug("Loaded {} settings from the {} JanusGraph config namespace", keysLoaded, ElasticSearchIndex.ES_CREATE_EXTRAS_NS);
        return settings;
    }

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchSetup.class);

    public abstract Connection connect(Configuration config) throws IOException;

    public static class Connection {

        private final ElasticSearchClient client;

        public Connection(ElasticSearchClient client) {
            this.client = Preconditions.checkNotNull(client, "Unable to instantiate Elasticsearch Client object");
        }

        public ElasticSearchClient getClient() {
            return client;
        }
    }
}
