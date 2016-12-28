package com.thinkaurelius.titan.graphdb;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.indexing.*;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_NS;

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
    public boolean supports(KeyInformation information, TitanPredicate titanPredicate) {
        return index.supports(information,titanPredicate);
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
