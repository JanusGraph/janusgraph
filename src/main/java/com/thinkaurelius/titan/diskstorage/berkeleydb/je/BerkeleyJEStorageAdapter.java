package com.thinkaurelius.titan.diskstorage.berkeleydb.je;

import com.thinkaurelius.titan.diskstorage.util.KeyValueStorageManager;
import com.thinkaurelius.titan.diskstorage.util.KeyValueStorageManagerAdapter;
import org.apache.commons.configuration.Configuration;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class BerkeleyJEStorageAdapter extends KeyValueStorageManagerAdapter {

    public BerkeleyJEStorageAdapter(Configuration config) {
        super(new BerkeleyJEStorageManager(config),config);
    }
}
