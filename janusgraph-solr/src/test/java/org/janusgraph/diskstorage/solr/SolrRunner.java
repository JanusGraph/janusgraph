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
import com.google.common.base.Strings;

import org.apache.commons.io.FileUtils;
import org.apache.solr.cloud.MiniSolrCloudCluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SolrRunner {

    public static final String ZOOKEEPER_URLS_SYSTEM_PROPERTY = System.getProperty("index.search.solr.zookeeper-url");

    protected static final int NUM_SERVERS = 1;
    protected static final String[] COLLECTIONS = readCollections();

    protected static final String[] KEY_FIELDS = new String[0];

    private static final String TMP_DIRECTORY = System.getProperty("java.io.tmpdir");
    private static final String TEMPLATE_DIRECTORY = "core-template";
    private static final String COLLECTIONS_FILE = "/collections.txt";

    private static MiniSolrCloudCluster miniSolrCloudCluster;

    public static void start() throws Exception {
        if (ZOOKEEPER_URLS_SYSTEM_PROPERTY != null) {
            return;
        }
        String userDir = System.getProperty("user.dir");
        String solrHome = userDir.contains("janusgraph-solr")
                ? Joiner.on(File.separator).join(userDir, "target", "test-classes", "solr")
                : Joiner.on(File.separator).join(userDir, "janusgraph-solr", "target", "test-classes", "solr");


        File templateDirectory = new File(solrHome + File.separator + TEMPLATE_DIRECTORY);
        assert templateDirectory.exists();

        File temp = new File(TMP_DIRECTORY + File.separator + "solr-" + System.nanoTime());
        assert !temp.exists();

        temp.mkdirs();
        temp.deleteOnExit();

        final String solrXml = new String(Files.readAllBytes(new File(solrHome, "solr.xml").toPath()));
        miniSolrCloudCluster = new MiniSolrCloudCluster(NUM_SERVERS, null, temp.toPath(), solrXml, null, null);

        for (String core : COLLECTIONS) {
            File coreDirectory = new File(temp.getAbsolutePath() + File.separator + core);
            assert coreDirectory.mkdirs();
            FileUtils.copyDirectory(templateDirectory, coreDirectory);
            miniSolrCloudCluster.uploadConfigSet(Paths.get(coreDirectory.getAbsolutePath()), core);
        }
    }

    public static String[] getZookeeperUrls() {
        final String[] zookeeperUrls;
        if (Strings.isNullOrEmpty(ZOOKEEPER_URLS_SYSTEM_PROPERTY)) {
            zookeeperUrls = new String[] { miniSolrCloudCluster.getZkServer().getZkAddress() };
        } else {
            zookeeperUrls = ZOOKEEPER_URLS_SYSTEM_PROPERTY.split(",");
        }
        return zookeeperUrls;
    }

    public static void stop() throws Exception {
        if (ZOOKEEPER_URLS_SYSTEM_PROPERTY != null) {
            return;
        }
        System.clearProperty("solr.solrxml.location");
        System.clearProperty("zkHost");
        miniSolrCloudCluster.shutdown();
    }

    private static String[] readCollections() {
        try (InputStream inputStream = SolrRunner.class.getResourceAsStream(COLLECTIONS_FILE);
             BufferedReader buffer = new BufferedReader(new InputStreamReader(inputStream))) {
            return Pattern.compile("\\s+").split(buffer.lines().collect(Collectors.joining("\n")));
        } catch (IOException e) {
            throw new RuntimeException("Unable to read collections file", e);
        }
    }
}
