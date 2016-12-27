package com.thinkaurelius.titan.diskstorage.es;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.util.system.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class ElasticsearchStatus {

    private static final Logger log =
            LoggerFactory.getLogger(ElasticsearchStatus.class);

    private final File file;
    private final int pid;

    public ElasticsearchStatus(File file, int pid) {
        Preconditions.checkNotNull(file);
        this.file = file;
        this.pid = pid;
    }

    public int getPid() {
        return pid;
    }

    public File getFile() {
        return file;
    }

    public static ElasticsearchStatus write(String path, int pid) {
        File f = new File(path);

        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(path);
            fos.write(String.format("%d", pid).getBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(fos);
        }

        return new ElasticsearchStatus(f, pid);
    }

    public static ElasticsearchStatus read(String path) {

        File pid = new File(path);

        if (!pid.exists()) {
            log.info("ES pidfile {} does not exist", path);
            return null;
        }

        BufferedReader pidReader = null;
        try {
            pidReader = new BufferedReader(new FileReader(pid));
            ElasticsearchStatus s = parsePidFile(pid, pidReader);
            log.info("Read ES pid {} from {}", pid, path);
            return s;
        } catch (IOException e) {
            log.warn("Assuming ES is not running", e);
        } finally {
            IOUtils.closeQuietly(pidReader);
        }

        return null;
    }

    private static ElasticsearchStatus parsePidFile(File f, BufferedReader br) throws IOException {
        String l = br.readLine();

        if (null == l || "".equals(l.trim())) {
            throw new IOException("Empty HBase statusfile " + f);
        }

        final int pid;
        try {
            pid = Integer.valueOf(l.trim());
        } catch (NumberFormatException e) {
            throw new IOException(e);
        }

        return new ElasticsearchStatus(f, pid);
    }
}
