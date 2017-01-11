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

import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

public class BerkeleyStorageSetup extends StorageSetup {

    public static ModifiableConfiguration getBerkeleyJEConfiguration(String dir) {
        return buildGraphConfiguration()
                .set(STORAGE_BACKEND,"berkeleyje")
                .set(STORAGE_DIRECTORY, dir);
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
