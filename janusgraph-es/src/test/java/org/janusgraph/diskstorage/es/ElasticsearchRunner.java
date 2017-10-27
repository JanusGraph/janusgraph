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

package org.janusgraph.diskstorage.es;

import org.janusgraph.DaemonRunner;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.example.GraphOfTheGodsFactory;
import org.janusgraph.util.system.IOUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.janusgraph.diskstorage.es.ElasticSearchIndex.BULK_REFRESH;
import static org.janusgraph.diskstorage.es.ElasticSearchIndex.INTERFACE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_HOSTS;

/**
 * Start and stop a separate Elasticsearch server process.
 */
public class ElasticsearchRunner extends DaemonRunner<ElasticsearchStatus> {

    /**
     * External server can be used for testing by setting the "index.hostname" environment variable.
     *
     * <p><b>Warning: JanusGraph ("janusgraph*") indices will be deleted from the server during testing.</b></p>
     */
    private static final String HOSTNAME = System.getProperty(ConfigElement.getPath(INDEX_HOSTS, "search"));

    public static final int PORT = 9200;

    private static final String DEFAULT_HOME_DIR = ".";

    private final String homedir;

    private ElasticMajorVersion majorVersion;

    private static final Logger log =
            LoggerFactory.getLogger(ElasticsearchRunner.class);

    public static final String ES_PID_FILE = "/tmp/janusgraph-test-es.pid";

    public ElasticsearchRunner(String esHome) {
        final Pattern VERSION_PATTERN = Pattern.compile("elasticsearch.version=(.*)");
        String version = null;
        try (InputStream in = ElasticsearchRunner.class.getClassLoader().getResourceAsStream("janusgraph-es.properties")) {
            if (in != null) {
                try (Scanner s = new Scanner(in)) {
                    s.useDelimiter("\\A");
                    final Matcher m = VERSION_PATTERN.matcher(s.next());
                    if (m.find()) {
                        version = m.group(1);
                    }
                }
            }
        } catch (IOException e) { }
        if (version == null) {
            throw new RuntimeException("Unable to find Elasticsearch version");
        }
        majorVersion = ElasticMajorVersion.parse(version);
        this.homedir = esHome + File.separator + "target" + File.separator + "elasticsearch-" + version;
    }

    public ElasticsearchRunner() {
        this(DEFAULT_HOME_DIR);
    }

    @Override
    public ElasticsearchStatus start() {
        if (HOSTNAME == null) {
            return super.start();
        }
        return null;
    }

    @Override
    public void stop() {
        if (HOSTNAME == null) {
            super.stop();
        }
    }

    @Override
    protected String getDaemonShortName() {
        return "Elasticsearch";
    }

    @Override
    protected void killImpl(ElasticsearchStatus stat) throws IOException {
        log.info("Killing {} pid {}...", getDaemonShortName(), stat.getPid());

        runCommand("/bin/kill", String.valueOf(stat.getPid()));

        log.info("Sent SIGTERM to {} pid {}", getDaemonShortName(), stat.getPid());

        try {
            watchLog(" closed", 60L, TimeUnit.SECONDS);
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        stat.getFile().delete();

        log.info("Deleted {}", stat.getFile());
    }

    @Override
    protected ElasticsearchStatus startImpl() throws IOException {
        File data = new File(homedir + File.separator + "data");
        File logs = new File(homedir + File.separator + "logs");

        if (data.exists() && data.isDirectory()) {
            log.info("Deleting {}", data);
            FileUtils.deleteDirectory(data);
        }

        if (logs.exists() && logs.isDirectory()) {
            log.info("Deleting {}", logs);
            FileUtils.deleteDirectory(logs);
        }

        runCommand(homedir + File.separator + "bin" + File.separator + "elasticsearch", "-d", "-p", ES_PID_FILE);
        try {
            watchLog(" started", 60L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return readStatusFromDisk();
    }

    @Override
    protected ElasticsearchStatus readStatusFromDisk() {
        return ElasticsearchStatus.read(ES_PID_FILE);
    }

    private void watchLog(String suffix, long duration, TimeUnit unit) throws InterruptedException {
        long startMS = System.currentTimeMillis();
        long durationMS = TimeUnit.MILLISECONDS.convert(duration, unit);
        long elapsedMS;

        File logFile = new File(homedir + File.separator + "logs" + File.separator + "elasticsearch.log");

        log.info("Watching ES logfile {} for {} token", logFile, suffix);

        while ((elapsedMS = System.currentTimeMillis() - startMS) < durationMS) {

            // Grep for a logline ending in the suffix and assume that means ES is ready
            BufferedReader br = null;
            try {
                br = new BufferedReader(new FileReader(logFile));
                String line;
                while (null != (line = br.readLine())) {
                    if (line.endsWith(suffix)) {
                        log.debug("Read line \"{}\" from ES logfile {}", line, logFile);
                        return;
                    }
                }
            } catch (FileNotFoundException e) {
                log.debug("Elasticsearch logfile {} not found", logFile, e);
            } catch (IOException e) {
                log.debug("Elasticsearch logfile {} could not be read", logFile, e);
            } finally {
                IOUtils.closeQuietly(br);
            }

            Thread.sleep(500L);
        }

        log.info("Elasticsearch logfile timeout ({} {})", elapsedMS, TimeUnit.MILLISECONDS);
    }

    ModifiableConfiguration setElasticsearchConfiguration(ModifiableConfiguration config, String index) {
        config.set(INTERFACE, ElasticSearchSetup.REST_CLIENT.toString(), index);
        config.set(INDEX_HOSTS, new String[] {getHostname() }, index);
        config.set(BULK_REFRESH, "wait_for", index);
        return config;
    }

    public String getHostname() {
        return HOSTNAME != null ? HOSTNAME : "127.0.0.1";
    }

    public ElasticMajorVersion getEsMajorVersion() {
        return majorVersion;
    }

    /**
     * Start Elasticsearch process, load GraphOfTheGods, and stop process. Used for integration testing.
     * @param args a singleton array containing a path to a JanusGraph config properties file
     */
    public static void main(String[] args) {
        final ElasticsearchRunner runner = new ElasticsearchRunner();
        runner.start();
        GraphOfTheGodsFactory.main(args);
        runner.stop();
    }

}
