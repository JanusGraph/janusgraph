package com.thinkaurelius.titan;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

import org.elasticsearch.common.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.util.system.IOUtils;

public class HBaseStatus {

    private static final Logger log =
            LoggerFactory.getLogger(HBaseStatus.class);

    private final File file;
    private final String version;
    private final int pid;

    private HBaseStatus(File file, String version, int pid) {
        Preconditions.checkNotNull(file);
        Preconditions.checkNotNull(version);
        this.file = file;
        this.version = version;
        this.pid = pid;
    }

    public String getVersion() {
        return version;
    }

    public int getPid() {
        return pid;
    }

    public File getFile() {
        return file;
    }

    public String getScriptDir() {
        return getScriptDirForHBaseVersion(version);
    }

    public static String getScriptDirForHBaseVersion(String hv) {
        if (hv.startsWith("0.94."))
            return "../titan-hbase-094/bin/";
        if (hv.startsWith("0.96."))
            return "../titan-hbase-096/bin/";
        if (hv.startsWith("0.98."))
            return "../titan-hbase-098/bin/";

        throw new RuntimeException("Unsupported HBase test version " + hv);
    }


    public static HBaseStatus read(String path) {

        File pid = new File(path);

        if (!pid.exists()) {
            log.info("HBase pidfile {} does not exist", path);
            return null;
        }

        BufferedReader pidReader = null;
        try {
            pidReader = new BufferedReader(new FileReader(pid));
            HBaseStatus s = parsePidFile(pid, pidReader);
            log.info("Read HBase status from {}", path);
            return s;
        } catch (HBasePidfileParseException e) {
            log.warn("Assuming HBase is not running", e);
        } catch (IOException e) {
            log.warn("Assuming HBase is not running", e);
        } finally {
            IOUtils.closeQuietly(pidReader);
        }

        return null;
    }

    public static HBaseStatus write(String path, String version) {
        File f = new File(path);

        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(path);
            fos.write(String.format("%s %d", version, 0).getBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(fos);
        }

        return new HBaseStatus(f, version, 0);
    }

    public void toFile(String path) {

        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(path);
            fos.write(String.format("%s %d", version, pid).getBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(fos);
        }
    }

    private static HBaseStatus parsePidFile(File f, BufferedReader br) throws HBasePidfileParseException, IOException {
        String l = br.readLine();

        if (null == l) {
            throw new HBasePidfileParseException("Empty HBase statusfile " + f);
        }

        String tokens[] = l.split(" ");

        if (2 != tokens.length) {
            throw new HBasePidfileParseException("Unable to parse HBase statusfile " + f + ": " + l);
        }

        String version = tokens[0];
        Integer processId;
        try {
             processId = Integer.valueOf(tokens[1]);
        } catch (NumberFormatException nfe) {
            throw new HBasePidfileParseException("Illegal HBase process ID in statusfile " + f + ": \"" + tokens[1] + "\" (expected an integer)");
        }

        HBaseStatus stat = new HBaseStatus(f, version, processId);
        return stat;
    }

    private static class HBasePidfileParseException extends Exception {
        private static final long serialVersionUID = 1L;
        public HBasePidfileParseException(String message) {
            super(message);
        }
    }
}

