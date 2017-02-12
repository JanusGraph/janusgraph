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

package org.janusgraph.graphdb;

import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.indexing.*;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import java.util.List;
import java.util.Map;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_NS;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TestMockIndexProvider implements IndexProvider {

    public static final ConfigOption<Boolean> INDEX_MOCK_FAILADD = new ConfigOption<Boolean>(INDEX_NS,"fail-adds",
            "Sets the index provider to reject adding documents. FOR TESTING ONLY",
            ConfigOption.Type.LOCAL, false).hide();

    public static final ConfigOption<String> INDEX_BACKEND_PROXY = new ConfigOption<String>(INDEX_NS,"proxy-for",
            "Define the indexing backed to use for index support behind the mock proxy",
            ConfigOption.Type.GLOBAL, INDEX_BACKEND.getDefaultValue()).hide();

    private final IndexProvider index;
    private final boolean failAdds;

    public TestMockIndexProvider(Configuration config) {
        this.index = Backend.getImplementationClass(config, config.get(INDEX_BACKEND_PROXY),
                StandardIndexProvider.getAllProviderClasses());
        this.failAdds = config.get(INDEX_MOCK_FAILADD);
    }

    @Override
    public void register(String store, String key, KeyInformation information, BaseTransaction tx) throws BackendException {
        index.register(store,key,information,tx);
    }

    @Override
    public void mutate(Map<String, Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        if (!failAdds) index.mutate(mutations,informations,tx);
        else throw new TemporaryBackendException("Blocked mutation");
    }

    @Override
    public void restore(Map<String, Map<String, List<IndexEntry>>> documents, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        if (!failAdds) index.restore(documents, informations, tx);
        else throw new TemporaryBackendException("Blocked mutation");
    }

    @Override
    public List<String> query(IndexQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        return index.query(query,informations,tx);
    }

    @Override
    public Iterable<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        return index.query(query,informations,tx);
    }

    @Override
    public BaseTransactionConfigurable beginTransaction(BaseTransactionConfig config) throws BackendException {
        return index.beginTransaction(config);
    }

    @Override
    public void close() throws BackendException {
        index.close();
    }

    @Override
    public void clearStorage() throws BackendException {
        index.clearStorage();
    }

    @Override
    public boolean supports(KeyInformation information, JanusGraphPredicate janusgraphPredicate) {
        return index.supports(information,janusgraphPredicate);
    }

    @Override
    public boolean supports(KeyInformation information) {
        return index.supports(information);
    }

    @Override
    public String mapKey2Field(String key, KeyInformation information) {
        return index.mapKey2Field(key,information);
    }

    @Override
    public IndexFeatures getFeatures() {
        return index.getFeatures();
    }
}
