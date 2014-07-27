package com.thinkaurelius.titan.diskstorage.es;

import com.thinkaurelius.titan.DaemonRunner;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Start and stop a separate Elasticsearch server process.
 */
public class ElasticsearchRunner extends DaemonRunner<ElasticsearchStatus> {

    private final String homedir;

    private static final Logger log =
            LoggerFactory.getLogger(ElasticsearchRunner.class);

    public static final String ES_PID_FILE = "/tmp/titan-test-es.pid";

    public ElasticsearchRunner(String esHome) {
        this.homedir = esHome;
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

        stat.getFile().delete();

        log.info("Deleted {}", stat.getFile());
    }

    @Override
    protected ElasticsearchStatus startImpl() throws IOException {

        File data = new File(homedir + File.separator + "target" + File.separator + "es-data");

        if (data.exists() && data.isDirectory()) {
            log.info("Deleting {}", data);
            FileUtils.deleteDirectory(data);
        }

        runCommand(homedir + File.separator + "bin/elasticsearch", "-d", "-p", ES_PID_FILE);
        try {
            // I should really retry parsing the pidfile in a loop up to some timeout
            Thread.sleep(2000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return readStatusFromDisk();
    }

    @Override
    protected ElasticsearchStatus readStatusFromDisk() {
        return ElasticsearchStatus.read(ES_PID_FILE);
    }
}
