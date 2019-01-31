// Copyright 2018 JanusGraph Authors
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

package org.janusgraph.diskstorage.configuration.builder;

import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * Builder for {@link ModifiableConfiguration}
 */
public class ModifiableConfigurationBuilder {

    public ModifiableConfiguration buildGlobalWrite(WriteConfiguration config){
        return build(GraphDatabaseConfiguration.ROOT_NS, config, BasicConfiguration.Restriction.GLOBAL);
    }

    private ModifiableConfiguration build(ConfigNamespace root, WriteConfiguration config, BasicConfiguration.Restriction restriction){
        return new ModifiableConfiguration(root, config, restriction);
    }
}
