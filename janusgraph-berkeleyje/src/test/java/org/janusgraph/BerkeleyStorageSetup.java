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

package org.janusgraph;

import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.utilint.TestHookAdapter;
import org.janusgraph.diskstorage.berkeleyje.BerkeleyJEKeyValueStore;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;

import java.time.Instant;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DROP_ON_CLEAR;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TX_CACHE_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.buildGraphConfiguration;

public class BerkeleyStorageSetup extends StorageSetup {
    static {
        // BerkeleyDB support only hour-discrete ttl. For compatibility
        // with testing framework we speed up time and change ttl converter function
        // Addition option that can be impact to ttl timing EnvironmentConfig.ENV_TTL_CLOCK_TOLERANCE
        BerkeleyJEKeyValueStore.ttlConverter = ttl -> ttl;
        TTL.setTimeTestHook(new TestHookAdapter<Long>() {
            private static final long SECONDS_PER_HOUR = 60 * 60;
            private final long initial = Instant.now().toEpochMilli();

            @Override
            public synchronized Long getHookValue() {
                long delta = Instant.now().toEpochMilli() - initial;
                return initial + delta * SECONDS_PER_HOUR;
            }
        });
    }

    public static ModifiableConfiguration getBerkeleyJEConfiguration(String dir) {
        return buildGraphConfiguration()
                .set(STORAGE_BACKEND,"berkeleyje")
                .set(STORAGE_DIRECTORY, dir)
                .set(DROP_ON_CLEAR, false);
    }

    public static ModifiableConfiguration getBerkeleyJEConfiguration() {
        return getBerkeleyJEConfiguration(getHomeDir("berkeleyje"));
    }

    public static WriteConfiguration getBerkeleyJEGraphConfiguration() {
        return getBerkeleyJEConfiguration().getConfiguration();
    }

    public static ModifiableConfiguration getBerkeleyJEPerformanceConfiguration() {
        return getBerkeleyJEConfiguration()
                .set(STORAGE_TRANSACTIONAL,false)
                .set(TX_CACHE_SIZE,1000);
    }
}
