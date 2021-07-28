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

package org.janusgraph.diskstorage.solr;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.SolrContainer;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;

public class JanusGraphSolrContainer extends SolrContainer {
    private static final String DEFAULT_SOLR_VERSION = "8.9.0";
    private static final String DEFAULT_SOLR_IMAGE = "solr";
    private static final String COLLECTIONS = "store1 store2 vertex edge namev namee composite psearch esearch vsearch mi mixed index1 index2 index3 ecategory vcategory pcategory theIndex vertices edges booleanIndex dateIndex instantIndex uuidIndex randomMixedIndex collectionIndex nameidx oridx otheridx lengthidx";

    private static String getVersion() {
        String property = System.getProperty("solr.docker.version");
        if (property != null)
            return property;
        return DEFAULT_SOLR_VERSION;
    }

    private static String getSolrImage() {
        String property = System.getProperty("solr.docker.image");
        if (property != null)
            return property;
        return DEFAULT_SOLR_IMAGE;
    }

    public MountableFile GetDependency(String file) {
        String dependencyDirectory = System.getProperty("janusgraph.testdir");
        if (null == dependencyDirectory) {
            dependencyDirectory = "target" + File.separator + "dependency";
        }
        return MountableFile.forHostPath(dependencyDirectory + File.separator + file);
    }

    public JanusGraphSolrContainer() {
        super(getSolrImage() + ":" + getVersion());
        addFixedExposedPort(SOLR_PORT, SOLR_PORT);
        withClasspathResourceMapping("solr/core-template", "/opt/solr/mydata", BindMode.READ_ONLY);
        withCopyFileToContainer(GetDependency("spatial4j.jar"), "/opt/solr/server/solr-webapp/webapp/WEB-INF/lib/spatial4j.jar");
        withCopyFileToContainer(GetDependency("jts-core.jar"), "/opt/solr/server/solr-webapp/webapp/WEB-INF/lib/jts-core.jar");
        withZookeeper(true);
    }

    private String getZookeeperUrl(boolean internal) {
        if (internal)
            return "localhost:" + ZOOKEEPER_PORT;
        return getHost() + ":" + getZookeeperPort();
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        super.containerIsStarted(containerInfo);
        String[] s = COLLECTIONS.split("[\\s]+");
        for (String s1 : s) {
            String cmd = "/opt/solr/server/scripts/cloud-scripts/zkcli.sh -zkhost "
                + getZookeeperUrl(true)
                + " -cmd upconfig -confdir /opt/solr/mydata -confname "
                + s1;
            try {
                execInContainer(cmd.split("[\\s]+"));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public Configuration getLocalSolrTestConfig() {
        final String index = "solr";
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        return getLocalSolrTestConfig(config, index).restrictTo(index);
    }

    public Configuration getLocalSolrTestConfigOverwriteKerberos() {
        final String index = "solr";
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(SolrIndex.KERBEROS_ENABLED, true, index);
        return getLocalSolrTestConfig(config, index).restrictTo(index);
    }

    public ModifiableConfiguration getLocalSolrTestConfig(ModifiableConfiguration config, String... indexBackends) {
        for (String indexBackend : indexBackends) {
            config.set(INDEX_BACKEND, "solr", indexBackend);
            config.set(SolrIndex.ZOOKEEPER_URL, new String[]{getZookeeperUrl(false)}, indexBackend);
            config.set(SolrIndex.HTTP_URLS, new String[]{"http://" + getHost() + ":" + getSolrPort() + "/solr"}, indexBackend);
            config.set(SolrIndex.WAIT_SEARCHER, true, indexBackend);
            config.set(GraphDatabaseConfiguration.INDEX_MAX_RESULT_SET_SIZE, 3, indexBackend);
        }
        return config;
    }
}
