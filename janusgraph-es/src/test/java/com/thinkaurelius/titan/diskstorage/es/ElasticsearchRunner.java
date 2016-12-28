package com.thinkaurelius.titan.diskstorage.es;

import com.thinkaurelius.titan.DaemonRunner;
import com.thinkaurelius.titan.util.system.IOUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.TimeUnit;

/**
 * Start and stop a separate Elasticsearch server process.
 */
public class ElasticsearchRunner extends DaemonRunner<ElasticsearchStatus> {

    private final String homedir;

    private static final Logger log =
            LoggerFactory.getLogger(ElasticsearchRunner.class);

    public static final String ES_PID_FILE = "/tmp/titan-test-es.pid";
    private String configFile = "elasticsearch.yml";

    public ElasticsearchRunner() {
        this.homedir = ".";
    }

    public ElasticsearchRunner(String esHome) {
        this.homedir = esHome;
    }

    public ElasticsearchRunner(String esHome, String configFile) {
        this(esHome);
        this.configFile = configFile;
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

        File data = new File(homedir + File.separator + "target" + File.separator + "es-data");
        File logs = new File(homedir + File.separator + "target" + File.separator + "es-logs");

        if (data.exists() && data.isDirectory()) {
            log.info("Deleting {}", data);
            FileUtils.deleteDirectory(data);
        }

        if (logs.exists() && logs.isDirectory()) {
            log.info("Deleting {}", logs);
            FileUtils.deleteDirectory(logs);
        }

        runCommand(homedir + File.separator + "bin/elasticsearch", "-d", "-p", ES_PID_FILE, "-Des.config=" + homedir + File.separator + "config" + File.separator + configFile);
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

        File logFile = new File(homedir + File.separator + "target" + File.separator
                + "es-logs" + File.separator + "elasticsearch.log");

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

}
