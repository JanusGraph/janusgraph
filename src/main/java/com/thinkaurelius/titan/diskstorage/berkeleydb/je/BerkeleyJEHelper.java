package com.thinkaurelius.titan.diskstorage.berkeleydb.je;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.thinkaurelius.titan.util.system.IOUtils;

import java.io.File;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class BerkeleyJEHelper {
    
    public static void clearEnvironment(String directory) {
        clearEnvironment(new File(directory));
    }

    public static void clearEnvironment(File directory) {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        Environment env = new Environment(directory, envConfig);

        Transaction tx = null;
        for (String db : env.getDatabaseNames()) {
            env.removeDatabase(tx,db);
        }
        env.close();
        IOUtils.deleteFromDirectory(directory);
    }
    
}
