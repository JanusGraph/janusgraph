package org.janusgraph;


import org.janusgraph.core.JanusFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.ReadConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;
import org.janusgraph.util.system.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.time.Duration;

public class StorageSetup {

    //############ UTILITIES #############

    public static final String getHomeDir(String subdir) {
        String homedir = System.getProperty("janus.testdir");
        if (null == homedir) {
            homedir = "target" + File.separator + "db";
        }
        if (subdir!=null && !StringUtils.isEmpty(subdir)) homedir += File.separator + subdir;
        File homefile = new File(homedir);
        if (!homefile.exists()) homefile.mkdirs();
        return homedir;
    }

    public static final File getHomeDirFile() {
        return getHomeDirFile(null);
    }

    public static final File getHomeDirFile(String subdir) {
        return new File(getHomeDir(subdir));
    }

    public static final void deleteHomeDir() {
        deleteHomeDir(null);
    }

    public static final void deleteHomeDir(String subdir) {
        File homeDirFile = getHomeDirFile(subdir);
        // Make directory if it doesn't exist
        if (!homeDirFile.exists())
            homeDirFile.mkdirs();
        boolean success = IOUtils.deleteFromDirectory(homeDirFile);
        if (!success) throw new IllegalStateException("Could not remove " + homeDirFile);
    }

    public static ModifiableConfiguration getInMemoryConfiguration() {
        return buildGraphConfiguration().set(STORAGE_BACKEND, "inmemory").set(IDAUTHORITY_WAIT, Duration.ZERO);
    }

    public static JanusGraph getInMemoryGraph() {
        return JanusFactory.open(getInMemoryConfiguration());
    }

    public static WriteConfiguration addPermanentCache(ModifiableConfiguration conf) {
        conf.set(DB_CACHE, true);
        conf.set(DB_CACHE_TIME,0l);
        return conf.getConfiguration();
    }

    public static ModifiableConfiguration getConfig(WriteConfiguration config) {
        return new ModifiableConfiguration(ROOT_NS,config, BasicConfiguration.Restriction.NONE);
    }

    public static BasicConfiguration getConfig(ReadConfiguration config) {
        return new BasicConfiguration(ROOT_NS,config, BasicConfiguration.Restriction.NONE);
    }

}
