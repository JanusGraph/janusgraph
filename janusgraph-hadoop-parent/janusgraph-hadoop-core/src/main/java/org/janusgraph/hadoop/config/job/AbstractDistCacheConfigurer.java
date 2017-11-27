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

package org.janusgraph.hadoop.config.job;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for {@link org.janusgraph.hadoop.config.job.JobClasspathConfigurer}
 * implementations that use Hadoop's distributed cache to store push class files to the cluster.
 */
public abstract class AbstractDistCacheConfigurer {

//    public static enum FileCopyMode {
//
//        /**
//         * Copy a jar unless a file with the same name already exists in the staging
//         * directory of the Hadoop FileSystem.
//         */
//        FILENAME,
//
//        /**
//         * Copy a jar unless a file with the same name and same modify time already exists
//         * in the staging directory of the Hadoop FileSystem.
//         */
//        MODTIME,
//
//        /**
//         * Unconditionally copy all jars to the Hadoop FileSystem, even if they
//         * already exist at the destination and have up-to-date modify times.
//         */
//        ALWAYS;
//    }
//
//    public static final ConfigOption<Boolean> SKIP_LOCAL_COPIES =
//            new ConfigOption<Boolean>(JanusGraphHadoopConfiguration.JARCACHE_NS, "skip-local-copies",
//            "When this option is true and Hadoop is configured to use a LocalFileSystem as " +
//            "its default, JanusGraph will not attempt to copy jars from the classpath to the " +
//            "LocalFileSystem (which is redundant when using the local JobRunner)", ConfigOption.Type.MASKABLE, true);

    private final Conf conf;

    private static final String HDFS_TMP_LIB_DIR = "janusgraphlib";

    private static final Logger log =
            LoggerFactory.getLogger(AbstractDistCacheConfigurer.class);

    public AbstractDistCacheConfigurer(String mapReduceJarFilename) {
        this.conf = configureByClasspath(mapReduceJarFilename);
    }

    public String getMapredJar() {
        return conf.mapReduceJar;
    }

    public ImmutableList<Path> getLocalPaths() {
        return conf.paths;
    }

    protected Path uploadFileIfNecessary(FileSystem localFS, Path localPath, FileSystem destFS) throws IOException {

        // Fast path for local FS -- DistributedCache + local JobRunner seems copy/link files automatically
        if (destFS.equals(localFS)) {
            log.debug("Skipping file upload for {} (destination filesystem {} equals local filesystem)",
                    localPath, destFS);
            return localPath;
        }

        Path destPath = new Path(destFS.getHomeDirectory() + "/" + HDFS_TMP_LIB_DIR + "/" + localPath.getName());

        Stats fileStats = null;

        try {
            fileStats = compareModtimes(localFS, localPath, destFS, destPath);
        } catch (IOException e) {
            log.warn("Unable to read or stat file: localPath={}, destPath={}, destFS={}",
                    localPath, destPath, destFS);
        }

        if (fileStats != null && !fileStats.isRemoteCopyCurrent()) {
            log.debug("Copying {} to {}", localPath, destPath);
            destFS.copyFromLocalFile(localPath, destPath);
            if (null != fileStats.local) {
                final long mtime = fileStats.local.getModificationTime();
                log.debug("Setting modtime on {} to {}", destPath, mtime);
                destFS.setTimes(destPath, mtime, -1); // -1 means leave atime alone
            }
        }

        return destPath;
    }

    private Stats compareModtimes(FileSystem localFS, Path localPath, FileSystem destFS, Path destPath) throws IOException {
        Stats s = new Stats();
        s.local = localFS.getFileStatus(localPath);
        if (destFS.exists(destPath)) {
            s.dest = destFS.getFileStatus(destPath);
            if (null != s.dest && null != s.local) {
                long l = s.local.getModificationTime();
                long d = s.dest.getModificationTime();
                if (l == d) {
                    if (log.isDebugEnabled())
                        log.debug("File {} with modtime {} is up-to-date", destPath, d);
                } else if (l < d) {
                    log.warn("File {} has newer modtime ({}) than our local copy {} ({})", destPath, d, localPath, l);
                } else {
                    log.debug("Remote file {} exists but is out-of-date: local={} dest={}", destPath, l, d);
                }
            } else {
                log.debug("Unable to stat file(s): [LOCAL: path={} stat={}] [DEST: path={} stat={}]",
                        localPath, s.local, destPath, s.dest);
            }
        } else {
            log.debug("File {} does not exist", destPath);
        }
        return s;
    }

    private static Conf configureByClasspath(String mapReduceJarFilename) {
        final List<Path> paths = new LinkedList<>();
        final String classpath = System.getProperty("java.class.path");
        final String mrj = mapReduceJarFilename.toLowerCase();
        String mapReduceJarPath = null;
        for (String classPathEntry : classpath.split(File.pathSeparator)) {
            if (classPathEntry.toLowerCase().endsWith(".jar") || classPathEntry.toLowerCase().endsWith(".properties")) {
                paths.add(new Path(classPathEntry));
                if (classPathEntry.toLowerCase().endsWith(mrj)) {
                    mapReduceJarPath = classPathEntry;
                }
            }
        }
        return new Conf(paths, mapReduceJarPath);
    }

    private static class Conf {

        private final ImmutableList<Path> paths;
        private final String mapReduceJar;

        public Conf(List<Path> paths, String mapReduceJar) {
            this.paths = ImmutableList.copyOf(paths);
            this.mapReduceJar = mapReduceJar;
        }
    }


    private static class Stats {
        private FileStatus local;
        private FileStatus dest;

        private boolean isRemoteCopyCurrent() {
            return null != local && null != dest && dest.getModificationTime() == local.getModificationTime();
        }
    }

    // LocalFileSystem doesn't checksum, it just returns null, so this is useless
//    private boolean compareChecksums(FileSystem localFS, Path localPath, FileSystem destFS, Path destPath) throws IOException {
//        if (destFS.exists(destPath)) {
//            FileChecksum localCheck = localFS.getFileChecksum(localPath);
//            FileChecksum destCheck = destFS.getFileChecksum(destPath);
//            if (null != destCheck && null != localCheck) {
//                byte[] db = destCheck.getBytes();
//                byte[] lb = localCheck.getBytes();
//                if (null != db && null != lb && Arrays.equals(db, lb)) {
//                    if (log.isDebugEnabled())
//                        log.debug("Checksum {} for file {} is up-to-date", Arrays.toString(db), destPath);
//                    return true;
//                } else {
//                    log.debug("Checksum mismatch on file {}: local={} dest={}", destPath, lb, db);
//                }
//            } else {
//                log.debug("Unable to checksum files: localPath={} localCheck={}, destPath={} destCheck={}",
//                        localPath, localCheck, destPath, destCheck);
//            }
//        } else {
//            log.debug("File {} does not exist", destPath);
//        }
//        return false;
//    }
}
