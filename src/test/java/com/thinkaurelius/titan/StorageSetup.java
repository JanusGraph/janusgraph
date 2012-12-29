package com.thinkaurelius.titan;

import com.thinkaurelius.titan.diskstorage.cassandra.embedded.CassandraEmbeddedStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.util.system.IOUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;

import java.io.File;

public class StorageSetup {

	public static final String cassandraYamlPath = StringUtils.join(
			new String[] { "file://", System.getProperty("user.dir"), "target",
					"cassandra-tmp", "conf", "127.0.0.1", "cassandra.yaml" },
			File.separator);
    public static final String cassandraOrderedYamlPath = StringUtils.join(
            new String[] { "file://", System.getProperty("user.dir"), "target",
                    "cassandra-tmp", "conf", "127.0.0.1", "cassandra-ordered.yaml" },
            File.separator);
	
    public static final String getHomeDir() {
        String homedir = System.getProperty("titan.testdir");
        if (null ==  homedir) {
             homedir = System.getProperty("java.io.tmpdir") + File.separator +  "titan-test";
        }
        File homefile = new File(homedir);
        if (!homefile.exists()) homefile.mkdirs();
        return homedir;
    }
    
    public static final File getHomeDirFile() {
        return new File(getHomeDir());
    }
	
	public static final void deleteHomeDir() {
		File homeDirFile = getHomeDirFile();
		// Make directory if it doesn't exist
		if (!homeDirFile.exists())
			homeDirFile.mkdirs();
		boolean success = IOUtils.deleteFromDirectory(homeDirFile);
        if (!success) throw new IllegalStateException("Could not remove " + homeDirFile) ;
	}

    public static Configuration getLocalStorageConfiguration() {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY,getHomeDir());
        config.addProperty(GraphDatabaseConfiguration.EXISTING_DIRECTORY_KEY, "true");
        return config;
    }
    
    public static Configuration getBerkeleyJEStorageConfiguration() {
        return getLocalStorageConfiguration();
    }

    public static Configuration getHBaseStorageConfiguration() {
        return getLocalStorageConfiguration();
    }

    public static Configuration getCassandraStorageConfiguration() {
        Configuration config = getLocalStorageConfiguration();
        return config;
    }
    
    public static Configuration getEmbeddedCassandraStorageConfiguration() {
        Configuration config = getLocalStorageConfiguration();
        config.addProperty(
        		CassandraEmbeddedStoreManager.CASSANDRA_CONFIG_DIR_KEY,
                cassandraOrderedYamlPath);
        return config;
    }


    //------

    public static Configuration getLocalGraphConfiguration() {
        BaseConfiguration config = new BaseConfiguration();
        config.setProperty(GraphDatabaseConfiguration.keyInNamespace(GraphDatabaseConfiguration.STORAGE_NAMESPACE, GraphDatabaseConfiguration.EXISTING_DIRECTORY_KEY), "true");
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY,getHomeDir());
        return config;
    }

    public static Configuration getBerkeleyJEGraphConfiguration() {
        Configuration config = getLocalGraphConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY,"berkeleyje");
        return config;
    }

    public static Configuration getHBaseGraphConfiguration() {
        Configuration config = StorageSetup.getLocalGraphConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY,"hbase");
        return config;
    }
    
    public static Configuration getAstyanaxGraphConfiguration() {
        Configuration config = StorageSetup.getLocalGraphConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY,"astyanax");
        return config;
    }

    public static Configuration getCassandraGraphConfiguration() {
        Configuration config = StorageSetup.getLocalGraphConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY,"cassandra");
        return config;
    }

    public static Configuration getCassandraThriftGraphConfiguration() {
        Configuration config = StorageSetup.getLocalGraphConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY,"cassandrathrift");
        return config;
    }

    public static Configuration getEmbeddedCassandraGraphConfiguration() {
        Configuration config = StorageSetup.getLocalGraphConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY,"embeddedcassandra");
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(
                CassandraEmbeddedStoreManager.CASSANDRA_CONFIG_DIR_KEY,
                cassandraYamlPath);
        return config;
    }

    public static Configuration getEmbeddedCassandraPartitionGraphConfiguration() {
        Configuration config = StorageSetup.getLocalGraphConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY,"embeddedcassandra");
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(
        		CassandraEmbeddedStoreManager.CASSANDRA_CONFIG_DIR_KEY,
                cassandraOrderedYamlPath);
        config.subset(GraphDatabaseConfiguration.IDS_NAMESPACE).addProperty(GraphDatabaseConfiguration.IDS_PARTITION_KEY,true);
        config.subset(GraphDatabaseConfiguration.IDS_NAMESPACE).addProperty(GraphDatabaseConfiguration.IDS_FLUSH_KEY,false);
//        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_IS_ORDERED_KEY,true);
        return config;
    }
}
