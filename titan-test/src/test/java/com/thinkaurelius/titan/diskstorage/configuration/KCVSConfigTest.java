package com.thinkaurelius.titan.diskstorage.configuration;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.configuration.backend.KCVSConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager;
import org.apache.commons.configuration.BaseConfiguration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class KCVSConfigTest extends WritableConfigurationTest {

    @Override
    public WriteConfiguration getConfig() {
        KeyColumnValueStoreManager manager = new InMemoryStoreManager(new BaseConfiguration());
        try {
            return new KCVSConfiguration(manager,"titan","general");
        } catch (StorageException e) {
            throw new RuntimeException(e);
        }
    }
}
