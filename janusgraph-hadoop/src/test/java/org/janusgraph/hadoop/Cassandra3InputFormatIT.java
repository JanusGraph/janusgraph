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

package org.janusgraph.hadoop;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;

import java.io.IOException;

public class Cassandra3InputFormatIT extends CassandraInputFormatIT {

    @Override
    protected PropertiesConfiguration getGraphConfiguration() throws ConfigurationException, IOException {
        final PropertiesConfiguration config = super.getGraphConfiguration();
        config.setProperty("gremlin.hadoop.graphInputFormat", "org.janusgraph.hadoop.formats.cassandra.Cassandra3InputFormat");
        return config;
    }

    @Override
    public WriteConfiguration getConfiguration() {
        String className = CassandraInputFormatIT.class.getSimpleName();
        return CassandraStorageSetup.getEmbeddedOrThriftConfiguration(className).getConfiguration();
    }
}
