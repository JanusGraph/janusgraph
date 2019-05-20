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

import com.google.common.base.Joiner;
import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.BeforeClass;
import java.io.File;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;

public class ThriftSolrTest extends SolrJanusGraphIndexTest {

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config =
                CassandraStorageSetup.getCassandraThriftConfiguration(ThriftSolrTest.class.getName());
        //Add index
        config.set(SolrIndex.ZOOKEEPER_URL, SolrRunner.getZookeeperUrls(), INDEX);
        config.set(SolrIndex.WAIT_SEARCHER, true, INDEX);
        config.set(INDEX_BACKEND,"solr",INDEX);
        config.set(GraphDatabaseConfiguration.INDEX_MAX_RESULT_SET_SIZE, 3, INDEX);
        //TODO: set SOLR specific config options
        return config.getConfiguration();
    }

    @BeforeClass
    public static void beforeClass() {
        String userDir = System.getProperty("user.dir");
        String cassandraDirFormat = Joiner.on(File.separator).join(userDir, userDir.contains("janusgraph-solr")
                                        ? "target" : "janusgraph-solr/target", "cassandra", "%s", "localhost-murmur");

        System.setProperty("test.cassandra.confdir", String.format(cassandraDirFormat, "conf"));
        System.setProperty("test.cassandra.datadir", String.format(cassandraDirFormat, "data"));

        CassandraStorageSetup.startCleanEmbedded();
    }


    /*
    The following two test cases do not pass for Solr since there is no (efficient) way of checking
    whether the document has been deleted before doing an update which will re-create the document.
     */

    @Override
    public void testDeleteVertexThenAddProperty() throws BackendException {

    }


    @Override
    public void testDeleteVertexThenModifyProperty() throws BackendException {

    }

    @Override
    public boolean supportsWildcardQuery() {
        return false;
    }

}
