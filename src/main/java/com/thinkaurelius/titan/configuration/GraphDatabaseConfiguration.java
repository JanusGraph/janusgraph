package com.thinkaurelius.titan.configuration;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.GraphDatabase;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.exceptions.GraphStorageException;
import com.thinkaurelius.titan.graphdb.database.GraphDB;
import com.thinkaurelius.titan.graphdb.database.StandardGraphDB;
import com.thinkaurelius.titan.graphdb.database.idassigner.NodeIDAssigner;
import com.thinkaurelius.titan.graphdb.database.idassigner.local.SimpleNodeIDAssigner;
import com.thinkaurelius.titan.graphdb.database.serialize.ObjectDiskStorage;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;
import com.thinkaurelius.titan.graphdb.database.statistics.InternalGraphStatistics;
import com.thinkaurelius.titan.graphdb.database.statistics.LocalGraphStatistics;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.idmanagement.ReferenceNodeIdentifier;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;

/**
 * Provides functionality to configure a {@link com.thinkaurelius.titan.core.GraphDatabase} instance.
 * 
 * GraphDatabaseConfiguration provides methods to set the configuration parameters used by the graph database instance
 * opened via the {@link #openDatabase()} method.
 * 
 * A graph database configuration is uniquely associated with a graph database and must not be used for multiple
 * databases. 
 * 
 * After a graph database has been initialized with respect to a configuration, some parameters of graph database
 * configuration may no longer be modifiable.
 * 
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public class GraphDatabaseConfiguration {
	
	private static final Logger log =
		LoggerFactory.getLogger(GraphDatabaseConfiguration.class);

	/**
	 * Name of the main configuration file inside the graph database's home directory.
	 * Default value = {@value}
	 */
	public static final String defaultConfigFileName = "config.txt";
	/**
	 * Name of the storage backend configuration file inside the graph database's home directory.
	 * Default value = {@value}
	 */
	public static final String defaultStorageConfigFileName = "storage.txt";
	
	/**
	 * Name of the registered classes persistence file inside the graph database's home directory.
	 * Default value = {@value}
	 */
	public static final String defaultRegisteredClassesFileName = "regClasses";
	
	/**
	 * Default local statistics filename. Default value = {@value}
	 */
	public static final String defaultLocalStatisticsFileName = "statistics.txt";
	
	/**
	 * Whether the graph database using locking by default.
	 * Default value = {@value}
	 */
	public static final boolean defaultTakeLocks = false;

	/**
	 * Whether to perform edge insertions and deletions in batches.
	 * Not all edge stores backends support the notion of batched writes.
	 * 
	 * Default value = {@value}
	 */
	public static final boolean defaultEdgeBatchWritingEnabled = false;
	
	/**
	 * Whether to perform property insertions and deletions in batches.
	 * Not all edge stores backends support the notion of batched writes. 
	 * 
	 * Default value = {@value}
	 */
	public static final boolean defaultPropertyBatchWritingEnabled = false;
	
	/**
	 * The maximum size to which a batch of edge insertions or deletions
	 * may grow before a write to the underlying edge store is forced.
	 * 
	 * This option only has an effect when
	 * {@link #getEdgeBatchWriteSize()} is true.
	 * 
	 * Default value = {@value}
	 */
	public static final int defaultEdgeBatchWriteSize = 4096;
	
	/**
	 * The maximum size to which a batch of property insertions or
	 * deletions may grow before a write to the underlying property store
	 * is forced.
	 * 
	 * This option only has an effect when
	 * {@link #getPropertyBatchWriteSize()} is true.
	 * 
	 * Default value = {@value}
	 */
	public static final int defaultPropertyBatchWriteSize = 4096;
	
	private static final double version = 0.2;
	
	private int inserterID=0;
	private int maxNoInserter=1;
	
	private boolean takeLocks=defaultTakeLocks;

	private boolean edgeBatchWritingEnabled=defaultEdgeBatchWritingEnabled;
	
	private boolean propertyBatchWritingEnabled=defaultPropertyBatchWritingEnabled;
	
	private int edgeBatchWriteSize=defaultEdgeBatchWriteSize;
	
	private int propertyBatchWriteSize=defaultPropertyBatchWriteSize;
	
	private ArrayList<RegisteredAttributeClass<?>> registeredAttributes = new ArrayList<RegisteredAttributeClass<?>>();
	
	private StorageConfiguration storage = new BerkeleyDBJeConfiguration();
	
	private Serializer serializer = null;
	
	private final ObjectDiskStorage objectStore;
	
	private final File homeDir;

	
	/**
	 * Initializes the graph database configuration and sets the home directory for the graph database.
	 * The home directory must be used for only graph database instance and should not contain 
	 * any files other than those created by the database for configuration and persistence.
	 * 
	 * @param home Home directory
	 * @throws IllegalArgumentException if home is not valid directory or cannot be opened.
	 */
	public GraphDatabaseConfiguration(File home) {
		Preconditions.checkNotNull(home,"Need to specify a directory");
		Preconditions.checkArgument(home.isDirectory(),home + " is not a directory!");
		homeDir = home;
		objectStore = new ObjectDiskStorage(this);
	}

	/**
	 * Initializes the graph database configuration and sets the home directory for the graph database.
	 * The home directory must be used for only graph database instance and should not contain 
	 * any files other than those created by the database for configuration and persistence.
	 * 
	 * @param home Home directory path
	 * @throws IllegalArgumentException if home is not valid directory or cannot be opened.
	 */
	public GraphDatabaseConfiguration(String home) {
		this(new File(home));
	}
	
	private boolean locked = false;
	
	void lock() {
		locked=true;
	}
	
	void verifySetting() {
		if (locked) throw new IllegalStateException("This configuration is locked and cannot be modified!");
	}
	
	/**
	 * Configures the local id manager used by the database.
	 * By default, the local id manager is configured for only a single inserter.
	 * When multiple instances of a graph database should be allowed to insert data into the graph
	 * simultaneously, each instance needs to be assigned a unique inserter ID and the maximum number
	 * of inserters needs to be bigger than any of those ids and may not be changed later.
	 * 
	 * @param inserterID Unique id for this inserter
	 * @param maxNoInserter A static upper bound for all unique inserter ids.
	 */
	public void setLocalIDManagement(int inserterID, int maxNoInserter) {
		verifySetting();
		this.inserterID=inserterID;
		this.maxNoInserter=maxNoInserter;
	}
	

	/**
	 * Configures the database to take locks for higher consistency guarantees.
	 * 
	 * If the underlying storage backend does not support ACID transactions, one can configure the
	 * database to use its own locking mechanism for higher consistency guarantees at the cost of 
	 * slightly lower performance.
	 * 
	 * <i>This feature is not yet supported</i>
	 * 
	 * @param takeLocks Whether to take locks.
	 */
	public void setTakeLocks(boolean takeLocks) {
		verifySetting();
		this.takeLocks = takeLocks;
	}
	
	/**
	 * Checks whether the database is configured to take locks
	 * 
	 * @return true, if the database using its own locking mechanism, else false
	 * @see #setTakeLocks(boolean)
	 */
	public boolean takeLocks() {
		return takeLocks;
	}

	/**
	 * Enable or disable batched insertion and deletion of edges on
	 * the database's underlying storage.
	 * 
	 * <p>
	 * 
	 * When true, the graph database uses special methods
	 * to insert and delete edges.  The graph database waits until it
	 * has accumulated up to {@link #getEdgeBatchWriteSize()} edge deletions
	 * or insertions before calling either {@code insertBatch()} or
	 * {@code deleteBatch()}.
	 * 
	 * <p>
	 * 
	 * When false, the graph database immediately and individually writes
	 * each edge insertion and deletion via the methods
	 * 
	 * <ul>
	 * <li>{@link KeyColumnValueStore#insert(java.nio.ByteBuffer, java.util.List, com.thinkaurelius.titan.diskstorage.TransactionHandle)}
	 * <li>{@link KeyColumnValueStore#delete(java.nio.ByteBuffer, java.util.List, com.thinkaurelius.titan.diskstorage.TransactionHandle)}
	 * </ul>
	 * 
	 * @param enabled whether to batch edge insertions and deletions
	 */
	public void setEdgeBatchWritingEnabled(boolean enabled) {
//		verifySetting();
		edgeBatchWritingEnabled = enabled;
	}

	/**
	 * Check whether batching of edge writes is enabled.
	 * 
	 * @return true if batching is enabled, false otherwise
	 */
	public boolean isEdgeBatchWritingEnabled() {
		return edgeBatchWritingEnabled;
	}
	
	/**
	 * Enable or disable batched insertion and deletion of properties on
	 * the database's underlying storage.
	 * 
	 * The javadoc for {@link #setEdgeBatchWritingEnabled(boolean)}
	 * applies to this method when the word "edge" is replaced with
	 * the word "property".
	 * 
	 * @param enabled whether to batch property insertions and deletions
	 */
	public void setPropertyBatchWritingEnabled(boolean enabled) {
//		verifySetting();
		propertyBatchWritingEnabled = enabled;
	}
	
	/**
	 * Check whether batching of property writes is enabled.
	 * 
	 * @return true if batching is enabled, false otherwise
	 */
	public boolean isPropertyBatchWritingEnabled() {
		return propertyBatchWritingEnabled;
	}
	
	/**
	 * The maximum number of edge insertions or deletions to batch
	 * before writing the batch to underlying storage.
	 * 
	 * This option only has an effect when {@link #isEdgeBatchWritingEnabled()}
	 * is true.
	 * 
	 * @return maximum size of edge write batches
	 */
	public int getEdgeBatchWriteSize() {
		return edgeBatchWriteSize;
	}

	/**
	 * Set the maximum number of edge insertions or deletions to batch
	 * before writing the batch to underlying storage.
	 * 
	 * This option only has an effect when {@link #isEdgeBatchWritingEnabled()}
	 * is true.
	 * 
	 * <p>
	 * The size limit applies separately to insertions and deletions.  In other
	 * words, the system may simultaneously batch up to
	 * {@code edgeBatchWriteSize - 1} edge insertions and
	 * {@code edgeBatchWriteSize - 1} edge deletions.
	 * 
	 * @param edgeBatchWriteSize maximum number of edge insertions or deletions
	 * 	                         to batch before forcing a write to underlying
	 * 	                         edge storage
	 */
	public void setEdgeBatchWriteSize(int edgeBatchWriteSize) {
//		verifySetting();
		if (0 > edgeBatchWriteSize)
			throw new IllegalArgumentException("Edge-batch write size cannot be negative");
		
		this.edgeBatchWriteSize = edgeBatchWriteSize;
	}

	/**
	 * The maximum number of property insertions or deletions to batch
	 * before writing the batch to underlying storage.
	 * 
	 * This option only has an effect when
	 * {@link #isPropertyBatchWritingEnabled()} is true.
	 * 
	 * @return maximum size of property write batches
	 */
	public int getPropertyBatchWriteSize() {
		return propertyBatchWriteSize;
	}

	/**
	 * Set the maximum number of property insertions or deletions to batch
	 * before writing the batch to underlying storage.
	 * 
	 * This option only has an effect when
	 * {@link #isPropertyBatchWritingEnabled()} is true.
	 * 
	 * <p>
	 * The size limit applies separately to insertions and deletions.  In other
	 * words, the system may simultaneously batch up to
	 * {@code propertyBatchWriteSize - 1} property insertions and
	 * {@code propertyBatchWriteSize - 1} property deletions.
	 * 
	 * @param propertyBatchWriteSize maximum number of property insertions or
	 * 							 deletions to batch before forcing a write to
	 * 							 underlying property storage
	 */
	public void setPropertyBatchWriteSize(int propertyBatchWriteSize) {
//		verifySetting();
		if (0 > propertyBatchWriteSize)
			throw new IllegalArgumentException("Edge-batch write size cannot be negative");
		
		this.propertyBatchWriteSize = propertyBatchWriteSize;
	}

	/**
	 * Registers a class to be used as a property data type, query load class or result item class
	 * to facilitate higher serialization performance.
	 * 
	 * @param clazz Class to register with the serializer.
	 * @see #registerClass(Class, AttributeSerializer)
	 */
	public<T> void registerClass(Class<T> clazz) {
		Preconditions.checkNotNull(clazz, "Registered class cannot be null.");
		verifySetting();
		RegisteredAttributeClass<T> reg = new RegisteredAttributeClass<T>(clazz);
		Preconditions.checkArgument(!registeredAttributes.contains(reg),"Class has already been registered!");
		registeredAttributes.add(reg);

	}
	
	/**
	 * Registers a class to be used as a property data type, query load class or result item class
	 * to facilitate higher serialization performance by using a custom {@link AttributeSerializer} as specified.
	 * 
	 * @param clazz Class to register for custom serialization
	 * @param serializer Custom AttributeSerializer to use for this class
	 */
	public<T> void registerClass(Class<T> clazz, AttributeSerializer<T> serializer) {
		Preconditions.checkNotNull(clazz,"Registered class cannot be null.");
		Preconditions.checkNotNull(serializer,"Registered serializer cannot be null.");
		verifySetting();
		RegisteredAttributeClass<T> reg = new RegisteredAttributeClass<T>(clazz,serializer);
		Preconditions.checkArgument(!registeredAttributes.contains(reg),"Class has already been registered!");
		registeredAttributes.add(reg);
	}
	
	/**
	 * Returns an iterable over all registered classes.
	 * 
	 * @return Iterable over all registered classes (thus far)
	 * @see #registerClass(Class)
	 */
	public Iterable<Class<?>> getRegisteredClasses() {
		return Iterables.transform(registeredAttributes, new Function<RegisteredAttributeClass<?>,Class<?>>() {
			@Override
			public Class<?> apply(RegisteredAttributeClass<?> arg) {
				return arg.type;
			}
		});
	}
	
	/**
	 * Sets the storage configuration for the storage backend to be used.
	 * 
	 * @param storage Configuration of the storage backend.
	 */
	public void setStorage(StorageConfiguration storage) {
		verifySetting();
		this.storage=storage;
	}
	
	/**
	 * Recovers an improperly shutdown graph database instance.
	 * 
	 * If the graph database has been unexpectedly terminated or in any other way improperly shutdown,
	 * calling this method prior to opening the same instance again invokes recovery mechanisms.
	 * Note that the graph database instance may not start after improper shutdown in which case calling
	 * this method is required.
	 */
	public void recovery() {
		lock();
		String configFile = getFileName(defaultConfigFileName);		
		PropertiesConfiguration config = getPropertiesConfig(configFile);
		config.setProperty("locked", false);
	}
	
	 /***
	  * Opens a graph database according to this configuration.
	  * 
	  * The parameters set in this configuration are used to configure the graph database to be opened. This
	  * method chooses the most suitable implementation of a graph database for the given configuration parameters.
	  * 
	  * Once a graph database has been opened, this configuration is locked and certain parameters can no longer
	  * be modified.
	  * 
	  * @return A graph database handle
	  * @throws GraphStorageException if there is an error in the storage backend
	  */
	public GraphDatabase openDatabase() {
		lock();
		String configFile = getFileName(defaultConfigFileName);
		String storageFile = getFileName(defaultStorageConfigFileName);
		
		PropertiesConfiguration config = getPropertiesConfig(configFile);
		PropertiesConfiguration storageConfig = getPropertiesConfig(storageFile);
		
		Preconditions.checkArgument(!config.getBoolean("locked",false),"Database is locked and cannot be opened!");
		
		
		Preconditions.checkArgument(config.getDouble("version", version)==version,"The version of the configuration file does not match system version ["+version+"]");
		config.setProperty("version", version);
		Preconditions.checkArgument(config.getInt("inserterid", inserterID)==inserterID,"The inserterID does not match existing database!");
		config.setProperty("inserterid", inserterID);
		Preconditions.checkArgument(config.getInt("maxnoinserter", maxNoInserter)==maxNoInserter,"The max number of inserters does not match existing database!");
		config.setProperty("maxnoinserter", maxNoInserter);
		Preconditions.checkArgument(config.getBoolean("locking", takeLocks)==takeLocks,"Locking configuration does not match existing database!");
		config.setProperty("locking", takeLocks);
		String scClassName = storage.getClass().getName();
		Preconditions.checkArgument(config.getString("storageConfigClass",scClassName).equals(scClassName),"Storage configuration does not match existing database!");
		config.setProperty("storageConfigClass",scClassName);
		
		config.setProperty("locked", true);
		
		ArrayList<RegisteredAttributeClass<?>> registered =  objectStore.getObject(defaultRegisteredClassesFileName, new ArrayList<RegisteredAttributeClass<?>>(0));
		for (RegisteredAttributeClass<?> clazz : registeredAttributes) {
			if (!registered.contains(clazz)) registered.add(clazz);
		}
		registeredAttributes=registered;
		objectStore.putObject(defaultRegisteredClassesFileName, registeredAttributes);
		
		storage.open(storageConfig);
		storage.save(storageConfig);


        NodeIDAssigner idAssigner = createIDAssigner();
		StorageManager storageManager = getStorageManager();
		
		OrderedKeyColumnValueStore edgeStore = storage.getEdgeStore(storageManager);
		OrderedKeyColumnValueStore propertyIndex = storage.getPropertyIndex(storageManager);
		
		GraphDB db = new StandardGraphDB(this,
				 storageManager,edgeStore,propertyIndex,
				 getSerializer(),idAssigner,getStatistics());
		return db;
	}
	
	private NodeIDAssigner createIDAssigner() {
		ReferenceNodeIdentifier refNodeIdent;
		refNodeIdent = ReferenceNodeIdentifier.noReferenceNodes;
        IDManager idmanager = new IDManager(refNodeIdent);
		return new SimpleNodeIDAssigner(idmanager, inserterID, maxNoInserter, objectStore);
	}

	private StorageManager getStorageManager() {
		return storage.getStorageManager(getHomeDirectory(),false);
	}

	private InternalGraphStatistics getStatistics() {
		return new LocalGraphStatistics(getPropertiesConfig(getFileName(defaultLocalStatisticsFileName)));
	}
	
	private Serializer getSerializer() {
		if (serializer==null) {
			serializer = new KryoSerializer();
			for (RegisteredAttributeClass<?> clazz : registeredAttributes) {
				clazz.registerWith(serializer);
			}
		}
		return serializer;
	}

	
	/**
	 * Closes this configuration.
	 * 
	 * This method is invoked automatically when closing the graph database instance
	 * opened through this configuration.
	 * 
	 * This method should not be called prior to closing the graph database.
	 * 
	 */
	public void close() {
		getPropertiesConfig(getFileName(defaultConfigFileName)).setProperty("locked", false);
		locked = false;
	}
	
	/**
	 * Saves all configuration parameters in the provided configuration file.
	 */
	public void writeTo(PropertiesConfiguration config) {
		config.setProperty("version", version);
		config.setProperty("inserterid",inserterID);
		config.setProperty("maxnoinserter", maxNoInserter);
		config.setProperty("locking", takeLocks);
		config.setProperty("storageConfigClass", storage.getClass().getName());
		storage.save(config);
	}
	
	/* ----------------------------------------
	 Methods for writing/reading config files
	-------------------------------------------*/
	
	/***
	 * Returns the home directory for the graph database initialized in this configuration
	 * 
	 * @return Home directory for this graph database configuration
	 */
	public File getHomeDirectory() {
		return homeDir;
	}
	
	/***
	 * Returns the home directory path for the graph database initialized in this configuration
	 * 
	 * @return Home directory path for this graph database configuration
	 */
	public String getHomePath() {
		return getPath(homeDir);
	}
	
	/**
	 * Returns a subdirectory of this home directory as specified by the directory name
	 * @param dir Name of subdirectory
	 * @return Sub directory of this home directory
	 */
	public File getSubDirectory(String dir) {
		return getSubDirectory(getHomePath(), dir);
	}
	
	/**
	 * Returns a subdirectory path for this home directory as specified by the directory name
	 * @param dir Name of subdirectory
	 * @return Sub directory paht for this home directory
	 */
	public String getSubdirPath(String dir) {
		return getPath(getSubDirectory(dir));
	}
	
	private String getFileName(String file) {
		return getFileName(getPath(homeDir),file);
	}
	
	private static File getSubDirectory(String base, String sub) {
		File subdir = new File(base, sub);
		if (!subdir.exists()) {
			if (!subdir.mkdir()) {
				throw new IllegalArgumentException("Cannot create subdirectory: " + sub);
			}
		}
		assert subdir.exists() && subdir.isDirectory();
		return subdir;		
	}	
	
	private static String getFileName(String dir, String file) {
		if (!dir.endsWith(File.separator)) dir = dir + File.separator;
		return dir + file;
	}
	
	public static String getPath(File dir) {
		return dir.getAbsolutePath()+File.separator;
	}

	
	static boolean existsFile(String file) {
		return (new File(file)).isFile();
	}
	
	static PropertiesConfiguration getPropertiesConfig(String file) {
		PropertiesConfiguration config = new PropertiesConfiguration();
		if (existsFile(file)) {
			try {
				config.load(file);
			} catch (ConfigurationException e) {
				throw new IllegalArgumentException("Cannot load existing configuration file: " + e.getMessage());
			}
		}
		config.setFileName(file);
		config.setAutoSave(true);
		return config;
	}
}
