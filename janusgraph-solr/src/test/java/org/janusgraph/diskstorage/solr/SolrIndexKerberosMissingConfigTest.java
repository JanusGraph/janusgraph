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

import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SolrIndexKerberosMissingConfigTest {

	@BeforeClass
	public static void setUpMiniCluster() throws Exception {
		SolrRunner.start();
	}

	@AfterClass
	public static void tearDownMiniCluster() throws Exception {
		SolrRunner.stop();
	}

	/**
	 * This test needs to stand alone because the java.security.auth.login.config
	 * property is JVM wide and is required for the KDC to run.
	 * 
	 * @throws Exception
	 */
	@Test(expected = PermanentBackendException.class)
	public void testIndexInitializationFailsWhenMissingJavaSecurityAuthLoginConfig() throws Exception {
		new SolrIndex(getLocalSolrTestConfig());
	}

	protected Configuration getLocalSolrTestConfig() {
		final String index = "solr";
		final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();

		config.set(SolrIndex.ZOOKEEPER_URL, SolrRunner.getZookeeperUrls(), index);
		config.set(SolrIndex.WAIT_SEARCHER, true, index);
		config.set(GraphDatabaseConfiguration.INDEX_MAX_RESULT_SET_SIZE, 3, index);
		config.set(SolrIndex.KERBEROS_ENABLED, true, index);
		return config.restrictTo(index);
	}
}
