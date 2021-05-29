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

package org.janusgraph.diskstorage.common;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.util.DirectoryUtil;

import java.io.File;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_ROOT;

/**
 * Abstract Store Manager used as the basis for local StoreManager implementations.
 * Simplifies common configuration management.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class LocalStoreManager extends AbstractStoreManager {

    protected final File directory;

    public LocalStoreManager(Configuration storageConfig) throws BackendException {
        super(storageConfig);
        Preconditions.checkArgument(storageConfig.has(STORAGE_DIRECTORY) ||
                                    (storageConfig.has(STORAGE_ROOT) && storageConfig.has(GRAPH_NAME)),
                                    "Please supply configuration parameter \"%s\" or both \"%s\" and \"%s\".",
                                                  STORAGE_DIRECTORY.toStringWithoutRoot(),
                                                  STORAGE_ROOT.toStringWithoutRoot(),
                                                  GRAPH_NAME.toStringWithoutRoot()
                                    );
        if (storageConfig.has(STORAGE_DIRECTORY)) {
            final String storageDir = storageConfig.get(STORAGE_DIRECTORY);
            directory = DirectoryUtil.getOrCreateDataDirectory(storageDir);
        } else {
            final String storageRoot = storageConfig.get(STORAGE_ROOT);
            final String graphName = storageConfig.get(GRAPH_NAME);
            directory = DirectoryUtil.getOrCreateDataDirectory(storageRoot, graphName);
        }
    }
}
