package com.thinkaurelius.titan.configuration;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.net.NodeID2InetMapper;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.File;

/**
 * Common configuration of Berkeley DB storage backend for both, the API and the Java edition.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public abstract class BaseBerkeleyDBConfiguration extends AbstractStorageConfiguration {

	boolean isTransactional = false;
	boolean isPrivate = true;
	boolean batchLoading = false;
	
	File dbDirectory = null;
	
	
	@Override
	public NodeID2InetMapper getNodeIDMapper() {
		return null; //since this storage backend is not distributed
	}
	
	@Override
	public void open(PropertiesConfiguration config) {
		super.open(config);
		String dir = config.getString("directory", null);
		if (dir!=null) {
			dbDirectory = new File(dir);
			checkDir(dbDirectory);
		}
	}
	
	@Override
	public void save(PropertiesConfiguration config) {
		config.setProperty("transactional", isTransactional);
		config.setProperty("private", isPrivate);	
		if (dbDirectory!=null) {
			config.setProperty("directory", dbDirectory.getPath());
		}
	}
	
	/**
	 * Configures Berkeley DB to use ACID transactions.
	 * 
	 * @param transactional Whether to use ACID transactions
	 */
	public void setTransactional(boolean transactional) {
		verifyModifiable();
		isTransactional = transactional;
	}
	
	/**
	 * Checks whether Berkeley DB is configured to use ACID transactions.
	 * 
	 * @return true, if Berkeley DB uses ACID transactions, else false.
	 */
	public boolean isTransactional() {
		return isTransactional;
	}
	
	/**
	 * Configures Berkeley DB to operate in private mode.
	 * A Berkeley DB instance is private if the underlying database is only accessed by one process.
	 * 
	 * @param isprivate Whether the Berkeley DB instance is private
	 */
	public void setPrivate(boolean isprivate) {
		verifyModifiable();
		isPrivate = isprivate;
	}
	
	/**
	 * Checks whether Berkeley DB is configured to limit database access to one process
	 * 
	 * @return true, if Berkeley DB assumes only one process, else false.
	 */
	public boolean isPrivate() {
		return isPrivate;
	}
	
	File getDirectory(File parentDirectory) {
		if (dbDirectory==null) return parentDirectory;
		else return dbDirectory;
	}
	
	static final void checkDir(File directory) {
		Preconditions.checkArgument(directory==null || (directory.isDirectory()),"Specified directory does not exist or is not a directory.");
	}
	
	/**
	 * Configures Berkeley DB to store all database files in the specified directory. This directory must exist
	 * and the user must have the rights to write to this directory.
	 * Configuring the storage directory is optional. By default, Berkeley DB will store all data in the directory of
	 * the associated graph database instance. By specifying <strong>null</strong> as the argument to this method, one
	 * can revert to this default behavior.
	 * 
	 * This method is useful when multiple graph database instances, each of which has a distinct home directory, should
	 * access the same underlying Berkeley DB storage.
	 * 
	 * @param directory Directory where the Berkeley DB database files are stored.
	 */
	public void setDBDirectory(File directory) {	
		dbDirectory = directory;
	}
	
	
	/**
	 * Returns the directory where the Berkeley DB database files are stored.
	 * If null is returned, those files are stored in the home directory of this graph database instance.
	 * 
	 * @return The directory where the Berkeley DB database files are stored.
	 */
	public File getDBDirectory() {
		return dbDirectory;
	}
	
	/**
	 * Optimizes Berkeley DB for batch loading.
	 * In batch loading mode, write will be buffered in memory for increased efficiency. This also means that 
	 * data will not be immediately durable.
	 * 
	 * @param batchloading if true, data should be loading in batches
	 */
	public void setBatchLoading(boolean batchloading) {
		this.batchLoading = batchloading;
	}
	
	/**
	 * Checks whether Berkeley DB is optimized for batch loading.
	 * 
	 * @return true, if Berkeley DB is optimized for batch loading, else false
	 */
	public boolean isBatchLoading() {
		return batchLoading;
	}
	
}
