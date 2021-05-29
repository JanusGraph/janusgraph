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


import org.apache.commons.lang3.StringUtils;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.ReadConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;

import java.io.File;
import java.time.Duration;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DB_CACHE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DB_CACHE_TIME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DROP_ON_CLEAR;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.IDAUTHORITY_WAIT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ROOT_NS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.USE_MULTIQUERY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.buildGraphConfiguration;

public class StorageSetup {

    //############ UTILITIES #############

    public static String getHomeDir(String subDirectory) {
        String homeDirectory = System.getProperty("janusgraph.testdir");
        if (null == homeDirectory) {
            homeDirectory = "target" + File.separator + "db";
        }
        if (subDirectory!=null && !StringUtils.isEmpty(subDirectory)) homeDirectory += File.separator + subDirectory;
        File homeFile = new File(homeDirectory);
        if (!homeFile.exists()) homeFile.mkdirs();
        return homeDirectory;
    }

    public static ModifiableConfiguration getInMemoryConfiguration() {
        return buildGraphConfiguration()
            .set(STORAGE_BACKEND, "inmemory")
            .set(IDAUTHORITY_WAIT, Duration.ZERO)
            .set(DROP_ON_CLEAR, false);
    }

    public static JanusGraph getInMemoryGraph() {
        return JanusGraphFactory.open(getInMemoryConfiguration());
    }
    
    public static JanusGraph getInMemoryGraphWithMultiQuery() {
        return JanusGraphFactory.open(getInMemoryConfiguration().set(USE_MULTIQUERY, true));
    }

    public static WriteConfiguration addPermanentCache(ModifiableConfiguration conf) {
        conf.set(DB_CACHE, true);
        conf.set(DB_CACHE_TIME, 0L);
        return conf.getConfiguration();
    }

    public static ModifiableConfiguration getConfig(WriteConfiguration config) {
        return new ModifiableConfiguration(ROOT_NS,config, BasicConfiguration.Restriction.NONE);
    }

    public static BasicConfiguration getConfig(ReadConfiguration config) {
        return new BasicConfiguration(ROOT_NS,config, BasicConfiguration.Restriction.NONE);
    }

}
