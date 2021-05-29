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

package org.apache.solr.cloud;

import org.apache.solr.client.solrj.embedded.JettyConfig;

import java.nio.file.Path;
import java.util.Optional;

/**
* The constructor necessary to create a MiniSolrCloudCluster enabled for kerberos is protected
* and cannot be called directly. Extend the class to make that constructor visible.
*/
public class ConfigurableMiniSolrCloudCluster extends MiniSolrCloudCluster {

    /**
    * Create a MiniSolrCloudCluster.
    *
    * @param numServers number of Solr servers to start
    * @param baseDir base directory that the mini cluster should be run from
    * @param solrXml solr.xml file to be uploaded to ZooKeeper
    * @param jettyConfig Jetty configuration
    * @param zkTestServer ZkTestServer to use.  If null, one will be created
    * @param securityJson A string representation of security.json file (optional).
    *
    * @throws Exception if there was an error starting the cluster
    */
    public ConfigurableMiniSolrCloudCluster(int numServers, Path baseDir, String solrXml, JettyConfig jettyConfig,
    ZkTestServer zkTestServer, Optional<String> securityJson) throws Exception {
        super(numServers, baseDir, solrXml, jettyConfig, zkTestServer, securityJson);
    }
}
