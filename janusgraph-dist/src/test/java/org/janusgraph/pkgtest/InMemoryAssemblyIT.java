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

package org.janusgraph.pkgtest;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class InMemoryAssemblyIT extends AbstractJanusGraphAssemblyIT {

    @Override
    protected String getConfigPath() {
        return "conf/janusgraph-inmemory.properties";
    }

    @Override
    protected String getServerConfigPath() {
        return "conf/gremlin-server/gremlin-server.yaml";
    }

    @Override
    protected String getGraphName() {
        return "inmemory";
    }

    @Test
    @Disabled
    @Override
    public void testGettingStartedAgainstGremlinSh() {}

    @Test
    @Disabled
    @Override
    public void testGettingStartedAgainstGremlinShFull() {}
}
