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

package org.janusgraph.diskstorage.solr;

import com.google.common.base.Joiner;
import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(SolrTestCategory.KERBEROS_TESTS)
public class SolrIndexKerberosKeytabTest extends SolrIndexTest {

    private static final String KEYTAB_CONF = "jaas_keytab.conf";

    private static MiniKDC miniKDC;

    private static String jassConfigFilePath;

    @BeforeAll
    public static void setUpMiniCluster() throws Exception {
        // Start KDC
        String userDir = System.getProperty("user.dir");
        String solrHome = userDir.contains("janusgraph-solr")
            ? Joiner.on(File.separator).join(userDir, "target", "test-classes", "solr")
            : Joiner.on(File.separator).join(userDir, "janusgraph-solr", "target", "test-classes", "solr");
        jassConfigFilePath = Joiner.on(File.separator).join(solrHome, KEYTAB_CONF);
        miniKDC = new MiniKDC(jassConfigFilePath);
        miniKDC.start("127.0.0.1");
        SolrRunner.start(true);

        // java.security.auth.login.config is already set as part of the initialization of the KDC.
        // That property value will be leveraged for both the mini solr cloud cluster to start up
        // and for configuring the solr client for janus to connect to the mini solr cloud cluster.
    }

    @AfterAll
    public static void tearDownMiniCluster() throws Exception {
        SolrRunner.stop();
        miniKDC.close();
    }

    @BeforeEach
    public void ensureClientPrincipalIsConfigured() throws KrbException {
        miniKDC.createClientPrincipal();
        miniKDC.createClientKeyTab();
    }

    @Test
    public void testSingleStoreFailsWhenKeytabIsMissing() throws Exception {
        assertThrows(RemoteSolrException.class, () -> {
            // Remove client keytab so authentication will fail
            miniKDC.deleteClientKeyTab();
            super.singleStore();
        });
    }

    @Test
    public void testSingleStoreFailsWhenPrincipalDoesntExist() throws Exception {
        assertThrows(RemoteSolrException.class, () -> {
            // Remove client principal to simulate a revoked user so authentication will fail
            miniKDC.deleteClientPrincipal();
            super.singleStore();
        });
    }

    @Override
    protected Configuration getSolrTestConfig() {
        final String index = "solr";
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();

        config.set(SolrIndex.ZOOKEEPER_URL, SolrRunner.getZookeeperUrls(), index);
        config.set(SolrIndex.WAIT_SEARCHER, true, index);
        config.set(GraphDatabaseConfiguration.INDEX_MAX_RESULT_SET_SIZE, 3, index);
        config.set(SolrIndex.KERBEROS_ENABLED, true, index);
        return config.restrictTo(index);
    }
}
