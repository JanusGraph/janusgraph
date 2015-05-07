package com.thinkaurelius.titan;


import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.ReadConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;
import com.thinkaurelius.titan.util.system.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.time.Duration;

public class StorageSetup {

    //############ UTILITIES #############

    public static final String getHomeDir(String subdir) {
        String homedir = System.getProperty("titan.testdir");
        if (null == homedir) {
            homedir = "target" + File.separator + "db";
        }
        if (subdir!=null && !StringUtils.isEmpty(subdir)) homedir += File.separator + subdir;
        File homefile = new File(homedir);
        if (!homefile.exists()) homefile.mkdirs();
        return homedir;
    }

    public static final String getHomeDir() {
        return getHomeDir(null);
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

    public static TitanGraph getInMemoryGraph() {
        return TitanFactory.open(getInMemoryConfiguration());
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
