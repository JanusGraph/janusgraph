package com.thinkaurelius.titan;


import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.util.system.IOUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;

import java.io.File;

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

    public static TitanGraph getInMemoryGraph() {
        BaseConfiguration config = new BaseConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "inmemory");
        return TitanFactory.open(config);
    }


    public static Configuration addPermanentCache(Configuration conf) {
        Configuration cacheconf = conf.subset(GraphDatabaseConfiguration.CACHE_NAMESPACE);
        cacheconf.addProperty(GraphDatabaseConfiguration.DB_CACHE_KEY,true);
        cacheconf.addProperty(GraphDatabaseConfiguration.DB_CACHE_TIME_KEY,0);
        return conf;
    }

}
