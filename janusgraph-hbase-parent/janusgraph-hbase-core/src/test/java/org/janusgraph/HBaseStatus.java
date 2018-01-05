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

package org.janusgraph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.janusgraph.util.system.IOUtils;

public class HBaseStatus {

    private static final Logger log =
            LoggerFactory.getLogger(HBaseStatus.class);

    private final File file;
    private final String version;

    private HBaseStatus(File file, String version) {
        Preconditions.checkNotNull(file);
        Preconditions.checkNotNull(version);
        this.file = file;
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public File getFile() {
        return file;
    }

    public String getScriptDir() {
        return HBaseStorageSetup.getScriptDirForHBaseVersion(version);
    }

    public String getConfDir() {
        return HBaseStorageSetup.getConfDirForHBaseVersion(version);
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
        } catch (HBasePidfileParseException | IOException e) {
            log.warn("Assuming HBase is not running", e);
        } finally {
            IOUtils.closeQuietly(pidReader);
        }

        return null;
    }

    public static HBaseStatus write(String path, String hbaseVersion) {
        File f = new File(path);

        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(path);
            fos.write(String.format("%s", hbaseVersion).getBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(fos);
        }

        return new HBaseStatus(f, hbaseVersion);
    }

    private static HBaseStatus parsePidFile(File f, BufferedReader br) throws HBasePidfileParseException, IOException {
        String l = br.readLine();

        if (null == l || "".equals(l.trim())) {
            throw new HBasePidfileParseException("Empty HBase status file " + f);
        }

        return new HBaseStatus(f, l.trim());
    }

    private static class HBasePidfileParseException extends Exception {
        private static final long serialVersionUID = 1L;
        public HBasePidfileParseException(String message) {
            super(message);
        }
    }
}

