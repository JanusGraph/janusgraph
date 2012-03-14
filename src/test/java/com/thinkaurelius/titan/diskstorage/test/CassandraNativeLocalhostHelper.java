package com.thinkaurelius.titan.diskstorage.test;

//import com.thinkaurelius.titan.diskstorage.cassandra.direct.CassandraBinaryStorageManager;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class CassandraNativeLocalhostHelper {


	private static final File confDir = new File(StringUtils.join(
			new String[] { "target", "cassandra-tmp", "conf", "127.0.1.1" },
			File.separator
	));

	private static final File cassYaml = new File(confDir, "cassandra.yaml");

	private static final File logConf = new File(confDir,
			"log4j-empty.properties");

        private static final Logger log = 
                LoggerFactory.getLogger(CassandraNativeLocalhostHelper.class);
                	
	private static final String cassProp = "cassandra.config";
	private static final String logProp = "log4j.configuration";

//	private CassandraBinaryStorageManager manager;
	private String oldCassYaml;
	private String oldLogConf;
	
	private boolean mangled;
	
	public void mangleSystemProperties() {
		assert !mangled;
		
		oldCassYaml = System.getProperty(cassProp);
		oldLogConf = System.getProperty(logProp);

                log.isDebugEnabled();

		try {
			System.setProperty(cassProp, "file://" +
					cassYaml.getCanonicalPath());
			
//			System.clearProperty(logProp);
			System.setProperty(logProp, "file://" +
					logConf.getCanonicalPath());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		mangled = true;
	}
	
	public void restoreSystemProperties() {
		assert mangled;
		
		if (null == oldCassYaml)
			System.clearProperty(cassProp);
		else 
			System.setProperty(cassProp, oldCassYaml);
		
		if (null == oldLogConf)
			System.clearProperty(logProp);
		else
			System.setProperty(logProp, oldLogConf);
		
		mangled = false;
	}

//	public CassandraBinaryStorageManager start(String keyspace,
//			String columnFamily) throws IOException {
//		if (null != manager)
//			return manager;
//
//		mangleSystemProperties();
//
//		CassandraBinaryStorageManager cassManager =
//			new CassandraBinaryStorageManager(keyspace);
//		cassManager.dropDatabase(columnFamily);
//		manager = cassManager;
//
//		return manager;
//	}
	
	public void stop() {
//		manager.close();
		restoreSystemProperties();
	}
}
