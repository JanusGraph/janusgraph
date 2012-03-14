package com.thinkaurelius.titan.configuration;

import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Provides a barebone implementation of a {@link StorageConfiguration} to be used
 * as the base class for all storage configuration implementations.
 * 
 * Essentially, this class provides functionality to lock the configuration to avoid
 * further modification. 
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 */
public abstract class AbstractStorageConfiguration implements StorageConfiguration {
	
	private boolean locked = false;
	
	/**
	 * Look the configuration to prohibit further modifications after the configuration is in use.
	 */
	protected void lock() {
		locked=true;
	}
	
	/**
	 * Checks whether the settings in this configuration object can stil be modified.
	 * 
	 * Once a graph database has been opened with this configuration, the configuration is locked
	 * because parameter changes will no longer have an effect.
	 */
	protected void verifyModifiable() {
		if (locked) throw new IllegalStateException("This configuration is locked and cannot be modified!");
	}
	
	@Override
	public void open(PropertiesConfiguration config) {
		lock();
	}
	
	
}
