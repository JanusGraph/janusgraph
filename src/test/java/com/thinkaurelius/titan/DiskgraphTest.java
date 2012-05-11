package com.thinkaurelius.titan;

import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.util.test.IOUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.io.File;

public class DiskgraphTest {

	public static final String homeDir;
	public static final File homeDirFile;
	
	static {
		String d = System.getProperty("titan.testdir");
		if (null == d) {
			d = System.getProperty("java.io.tmpdir") + 
				File.separator +  "titan-test";
		}
		homeDir = d;
		homeDirFile = new File(homeDir);
		if (!homeDirFile.exists()) homeDirFile.mkdirs();
	}
	
	public static final void deleteHomeDir() {
		// Make directory if it doesn't exist
		if (!homeDirFile.exists())
			homeDirFile.mkdirs();
		boolean success = IOUtils.deleteFromDirectory(homeDirFile);
        if (!success) System.err.println("Could not delete " + homeDir);
	}

    public static Configuration getDirectoryStorageConfiguration() {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY,homeDir);
        return config;
    }

    public static Configuration getDefaultConfiguration() {
        BaseConfiguration config = new BaseConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY,homeDir);
        return config;
    }
	
}
