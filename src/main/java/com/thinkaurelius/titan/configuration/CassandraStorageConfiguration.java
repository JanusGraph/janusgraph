package com.thinkaurelius.titan.configuration;

import com.thinkaurelius.titan.core.GraphDatabase;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftNodeIDMapper;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager;
import com.thinkaurelius.titan.net.NodeID2InetMapper;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Configuration of the Cassandra Storage Backend.
 * <p>
 * Where a setter notes that "the configuration must be mutable",
 * this means the configuration cannot yet be in use by a running
 * {@link GraphDatabaseConfiguration} and/or associated
 * {@link GraphDatabase}.  See {@link GraphDatabaseConfiguration#verifySetting()}.
 * 
 * @author Daniel LaRocque (dalaro@umiacs.umd.edu)
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 */
public class CassandraStorageConfiguration extends AbstractStorageConfiguration {
	
	private static final Logger log =
		LoggerFactory.getLogger(CassandraStorageConfiguration.class);
	
	private static final String PROP_KEYSPACE = "keyspace";
	private static final String PROP_HOSTNAME = "hostname";
	private static final String PROP_PORT = "port";
	private static final String PROP_SELF_HOSTNAME = "selfHostname";
	
	/**
	 * Default name for the Cassandra keyspace
	 * <p>
	 * Value = {@value}
	 */
	public static final String DEFAULT_KEYSPACE = "titantest00";
	
	/**
	 * Default hostname at which to attempt Cassandra Thrift connection.
	 * <p>
	 * Value = {@value}
	 */
	public static final String DEFAULT_HOSTNAME = null;
	
	/**
	 * Default canonical hostname of the local machine.
	 * <p>
	 * Value = {@value}
	 */
	public static final String DEFAULT_SELF_HOSTNAME = null;
	
	/**
	 * Default timeout for Thrift TSocket objects used to
	 * connect to the Cassandra cluster.
	 * <p>
	 * Value = {@value}
	 */
	public static final int DEFAULT_THRIFT_TIMEOUT_MS = 10000;
	
	/**
	 * Default port at which to attempt Cassandra Thrift connection.
	 * <p>
	 * Value = {@value}
	 */
	public static final int DEFAULT_PORT = 9160;
	
	private String keyspace = DEFAULT_KEYSPACE;
	private String hostname = DEFAULT_HOSTNAME;
	private int port = DEFAULT_PORT;
	private String selfHostname = DEFAULT_SELF_HOSTNAME;
	private int thriftTimeoutMS = DEFAULT_THRIFT_TIMEOUT_MS;

	/**
	 * Initializes the Cassandra storage configuration with the default keyspace name
	 */
	public CassandraStorageConfiguration() {
		this(DEFAULT_KEYSPACE);
	}
	
	/**
	 * Initializes the Cassandra storage configuration with the given keyspace name
	 * 
	 * @param keyspaceName Name of the Cassandra keyspace
	 */
	public CassandraStorageConfiguration(String keyspaceName) {
		keyspace = keyspaceName;
	}

	/**
	 * The Cassandra keyspace to use.  The storage backend uses only
	 * this single keyspace.
	 * 
	 * @return the Cassandra keyspace
	 */
	public String getKeyspace() {
		return keyspace;
	}
	
	/**
	 * Set the Cassandra keyspace.
	 * <p>
	 * The configuration must be mutable.
	 * 
	 * @param keyspace The Cassandra keyspace
	 */
	public void setKeyspace(String keyspace) {
		verifyModifiable();
		this.keyspace = keyspace;
	}
	
	/**
	 * The Cassandra host to which to connect.
	 * <p>
	 * Legal values are hostnames as strings and null.
	 * <p>
	 * If non-null, then the string must resolve when
	 * InetAddress.getHostByName() is called with the string as an argument.
	 * Because InetAddress.getHostByName() is used to interpret string values, both
	 * hostnames and IP addresses are valid.  For instance, "localhost" and
	 * "127.0.0.1" are both valid and equivalent settings that
	 * configure the system to connect to a Cassandra instance
	 * running on the local computer.
	 * <p>
	 * Setting null is equivalent to setting the value of
	 * InetAddress.getLocalHost().getCanonicalHostName().
	 * On hosts configured with a working DNS configuration,
	 * that call returns the computer's full Internet hostname.
	 * Specifically, {@link #getRuntimeHostname()} is used by
	 * the Cassandra code to translate null into the canonical
	 * hostname or pass through a non-null String value as
	 * appropriate.
	 * 
	 * @see #getRuntimeHostname()
	 * @see #getPort()
	 * @return hostname as a string or null
	 */
	public String getHostname() {
		return hostname;
	}
	
	/**
	 * Set the hostname to attempt connection on via
	 * Cassandra's Thrift protocol.
	 * <p>
	 * The configuration must be mutable.
	 * 
	 * @see #setPort(int)
	 * @param hostname network hostname of Cassandra host
	 */
	public void setHostname(String hostname) {
		verifyModifiable();
		this.hostname = hostname;
	}

	/**
	 * The TCP port on which the storage backend attempts
	 * to connect to Cassandra.
	 * 
	 * @see #getHostname()
	 * @return the Cassandra TCP port
	 */
	public int getPort() {
		return port;
	}
	
	/**
	 * Set the port to attempt connection on via
	 * Cassandra's Thrift protocol.
	 * <p>
	 * The configuration must be mutable.
	 * 
	 * @see #setHostname(String)
	 * @param port TCP port of Cassandra service
	 */
	public void setPort(int port) {
		verifyModifiable();
		this.port = port;
	}
	
	/**
	 * The Thrift TSocket timeout.  TSocket in turn passes
	 * this value to java.net.Socket.setSoTimeout(int).
	 * <p>
	 * Consider increasing this setting from the default if
	 * operations fail with Thrift's "TimedOutException".
	 * Also consider increasing Cassandra's "rpc_timeout"
	 * configuration parameter.
	 * 
	 * @return TSocket timeout in milliseconds
	 */
	public int getThriftTimeoutMS() {
		return thriftTimeoutMS;
	}

	/**
	 * Set the Thrift TSocket timeout.
	 * <p>
	 * The configuration must be mutable.
	 * 
	 * @param thriftTimeoutMS timeout in milliseconds
	 */
	public void setThriftTimeoutMS(int thriftTimeoutMS) {
		verifyModifiable();
		this.thriftTimeoutMS = thriftTimeoutMS;
	}
	
	/**
	 * The canonical network hostname of the local machine.
	 * <p>
	 * This is interpreted in the same way as {@link #getHostname()}
	 * with respect to null value.
	 * <p>
	 * This parameter generally matters only when running Titan within
	 * a multi-node Cassandra cluster.
	 * <p>
	 * This value is compared to Cassandra's "Token Ring", which is
	 * a map of hostnames to the keys those hostnames have been
	 * assigned.  This value must match whatever the rest of the
	 * Cassandra cluster thinks the local machine is called.  Otherwise,
	 * the system can get confused about where to find data most
	 * efficiently.
	 * <p>
	 * The default is usually correct.  Generally, this
	 * only needs to be changed when there's a DNS resolver
	 * problem or for test configurations.
	 * 
	 * @return hostname as a string or null
	 */
	public String getSelfHostname() {
		return selfHostname;
	}
	
	/**
	 * Set the canonical network hostname of the local machine.
	 * <p>
	 * The configuration must be mutable.
	 * 
	 * @param selfHostname
	 */
	public void setSelfHostname(String selfHostname) {
		verifyModifiable();
		this.selfHostname = selfHostname;
	}

	/**
	 * If the hostname setting is null, returns the result of
	 * InetAddress.getLocalHost().getCanonicalHostName(); if
	 * the hostname setting is non-null, it is returned as-is.
	 * 
	 * @throws RuntimeException if InetAddress.getLocalHost()
	 *         throws UnknownHostException
	 * @return a guaranteed non-null String representing the
	 *         hostname of the Cassandra cluster node to which
	 *         the Cassandra backend will connect
	 */
	public String getRuntimeHostname() {
		return interpretHostname();
	}
	
	/**
	 * If the selfHostname setting is null, returns the result
	 * of InetAddress.getLocalHost().getCanonicalHostName(); if
	 * the hostname setting is non-null, it is returned as-is.
	 * 
	 * @throws RuntimeException if InetAddress.getLocalHost()
	 *         throws UnknownHostException
	 * @return a guaranteed non-null String representing the
	 *         hostname of the local machine
	 */
	public String getRuntimeSelfHostname() {
		return interpretHostname(getSelfHostname());
	}
	
	/**
	 * Like {@link #getRuntimeHostname()}, except using
	 * the static {@link #DEFAULT_HOSTNAME} field instead of
	 * the non-static {@link #hostname} field.
	 * 
	 * @see #getRuntimeHostname()
	 * @throws RuntimeException if InetAddress.getLocalHost()
	 *         throws UnknownHostException
	 * @return guaranteed non-null default Cassandra hostname
	 */
	public static String getDefaultRuntimeHostname() {
		return interpretHostname(DEFAULT_HOSTNAME);
	}
	
	/**
	 * Like {@link #getDefaultRuntimeHostname()}, except using
	 * the static {@link #DEFAULT_SELF_HOSTNAME} field instead of
	 * the non-static {@link #selfHostname} field.
	 * 
	 * @see #getRuntimeSelfHostname()
	 * @throws RuntimeException if InetAddress.getLocalHost()
	 *         throws UnknownHostException
	 * @return guaranteed non-null default self-hostname
	 */
	public static String getDefaultRuntimeSelfHostname() {
		return interpretHostname(DEFAULT_SELF_HOSTNAME);
	}
		
	@Override
	public CassandraThriftStorageManager getStorageManager(File directory, boolean readOnly) {
		/*
		 * Cassandra has no need for and ignores the directory argument
		 * of this method.  However, GraphDatabaseConfiguration calls
		 * this method with a valid (non-null) directory argument no
		 * matter the concrete implementation behind the interface, so
		 * it does not make sense to complain about the unused argument:
		 * we'd be complaining about our own internal classes using one
		 * another, and the end-user of this application wouldn't have the
		 * power to fix the problem anyway.  So, for now, silently drop
		 * the directory argument and leave it at that.
		 */
		
		return getStorageManager(readOnly);
	}
	
	/**
	 * This is a convenience method equivalent to calling
	 * {@link #getStorageManager(null, readOnly)}.
	 * 
	 * @param readOnly whether the storage manager is read-only
	 * @return the storage manager
	 */
	public CassandraThriftStorageManager getStorageManager(boolean readOnly) {
		if (readOnly)
			log.warn("Ignoring argument \"readOnly\"; only read-write "
				+ "access is currently supported by the Cassandra Thrift "
				+ "interface.");
	
		return new CassandraThriftStorageManager(this);
	}

	@Override
	public OrderedKeyColumnValueStore getEdgeStore(StorageManager manager) {
		return manager.openOrderedDatabase(StorageConfiguration.edgeStoreName);
	}

	@Override
	public OrderedKeyColumnValueStore getPropertyIndex(StorageManager manager) {
		return manager.openOrderedDatabase(StorageConfiguration.propertyIndexName);
	}

	@Override
	public void save(PropertiesConfiguration config) {
		config.setProperty(PROP_KEYSPACE, keyspace);
		config.setProperty(PROP_HOSTNAME, hostname);
		config.setProperty(PROP_PORT, port);
		config.setProperty(PROP_SELF_HOSTNAME, selfHostname);
	}
	
	@Override
	public void open(PropertiesConfiguration config) {
		this.keyspace = config.getString(PROP_KEYSPACE, keyspace);
		this.hostname = config.getString(PROP_HOSTNAME, hostname);
		this.port = config.getInteger(PROP_PORT, port);
		this.selfHostname = config.getString(PROP_SELF_HOSTNAME, selfHostname);
	}

	/**
	 * The {@code keyspace} field must be set properly before calling
	 * this method.  This method returns a mapper which functions only
	 * on node IDs within the {@code keyspace} set when this method
	 * was called.
	 */
	@Override
	public NodeID2InetMapper getNodeIDMapper() {
		return new CassandraThriftNodeIDMapper(keyspace, interpretHostname(), port, interpretSelfHostname(), getThriftTimeoutMS());
	}

	/**
	 * If hostname is non-null, returns hostname.
	 * 
	 * If hostname is null, returns the result of calling
	 * InetAddress.getLocalHost().getCanonicalHostName().
	 * Any exceptions generated during said call are rethrown as
	 * RuntimeException.
	 * 
	 * @throws RuntimeException in case of UnknownHostException for localhost
	 * @return sanitized hostname
	 */
	private static String interpretHostname(String hostname) {
		if (null == hostname) {
			try {
				return InetAddress.getLocalHost().getCanonicalHostName();
			} catch (UnknownHostException e) {
				throw new RuntimeException(e);
			}
		} else {
			return hostname;
		}
	}
	
	private String interpretHostname() {
		return interpretHostname(hostname);
	}
	
	private String interpretSelfHostname() {
		return interpretHostname(selfHostname);
	}
}
