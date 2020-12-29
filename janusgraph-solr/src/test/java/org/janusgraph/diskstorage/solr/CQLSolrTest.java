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

package org.janusgraph.diskstorage.solr;

import org.janusgraph.JanusGraphCassandraContainer;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class CQLSolrTest extends SolrJanusGraphIndexTest {

    @Container
    protected static JanusGraphSolrContainer solrContainer = new JanusGraphSolrContainer();

    @Container
    private static JanusGraphCassandraContainer cassandraContainer = new JanusGraphCassandraContainer();

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = cassandraContainer.getConfiguration(CQLSolrTest.class.getName());
        return solrContainer.getLocalSolrTestConfig(INDEX, config).getConfiguration();
    }

    @Override
    public boolean supportsWildcardQuery() {
        return false;
    }

}
