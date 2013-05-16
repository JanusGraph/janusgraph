package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.File;

/**
 * Implementation of storage backend properties using a local configuration file.
 *
 * Each storage backend provides the functionality to get and set properties for that particular backend.
 * This class implementation this feature using a local configuration file. Hence, it is only suitable for
 * local storage backends.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class FileStorageConfiguration {

    private static final String TITAN_CONFIG_FILE_NAME = "titan-config.properties";

    private final File directory;


    public FileStorageConfiguration(File directory) {
        Preconditions.checkNotNull(directory);
        Preconditions.checkArgument(directory.exists() && directory.isDirectory(),"Given path is not a directory: %s",directory);
        this.directory=directory;
    }

    public String getConfigurationProperty(String key) throws StorageException {
        File configFile = getConfigFile(directory);

        if (!configFile.exists()) //property has not been defined
            return null;

        Preconditions.checkArgument(configFile.isFile());
        try {
            Configuration config = new PropertiesConfiguration(configFile);
            return config.getString(key, null);
        } catch (ConfigurationException e) {
            throw new PermanentStorageException("Could not read from configuration file", e);
        }
    }

    public void setConfigurationProperty(String key, String value) throws StorageException {
        File configFile = getConfigFile(directory);

        try {
            PropertiesConfiguration config = new PropertiesConfiguration(configFile);
            config.setProperty(key, value);
            config.save();
        } catch (ConfigurationException e) {
            throw new PermanentStorageException("Could not save configuration file", e);
        }
    }

    private static File getConfigFile(File dbDirectory) {
        return new File(dbDirectory.getAbsolutePath() + File.separator + TITAN_CONFIG_FILE_NAME);
    }

}
