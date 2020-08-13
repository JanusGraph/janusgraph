// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.diskstorage.es;

import org.janusgraph.graphdb.JanusGraphIndexTest;
import org.testcontainers.junit.jupiter.Container;

public abstract class ElasticsearchJanusGraphIndexTest extends JanusGraphIndexTest {

    @Container
    protected static JanusGraphElasticsearchContainer esr = new JanusGraphElasticsearchContainer();

    public ElasticsearchJanusGraphIndexTest() {
        super(true, true, true);
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }

    @Override
    public boolean supportsWildcardQuery() {
        return true;
    }

    @Override
    protected boolean supportsCollections() {
        return true;
    }

    @Override
    public boolean supportsGeoPointExistsQuery() {
        return true;
    }
}
