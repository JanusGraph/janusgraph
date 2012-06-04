package com.thinkaurelius.titan.diskstorage.cassandra;

import org.apache.cassandra.thrift.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class starts a Thrift CassandraDaemon inside the current JVM.
 * 
 * @author dalaro
 *
 */
public class CassandraDaemonWrapper {
	
	private static Boolean started = false;
	
	private static final Logger log = LoggerFactory.getLogger(CassandraDaemonWrapper.class);
	
	private static String liveCassandraYamlPath;
	
	public static synchronized void start(String cassandraYamlPath) {
		if (started) {
			if (null != cassandraYamlPath &&
					!cassandraYamlPath.equals(liveCassandraYamlPath)) {
				log.warn("Can't start in-process Cassandra instance " +
						"with yaml path {} because an instance was " +
						"previously started with yaml path {}", 
						cassandraYamlPath, liveCassandraYamlPath);
			}
			
			return;
		}
		
		log.debug("Current working directory: {}", System.getProperty("user.dir"));
		
		System.setProperty("cassandra.config", cassandraYamlPath);
		System.setProperty("log4j.defaultInitOverride", "false");
		
		(new Thread(new CassandraRunner())).run();
		
		liveCassandraYamlPath = cassandraYamlPath;
		
		started = true;
	}
	
	private static class CassandraRunner implements Runnable {
		@Override
		public void run() {
			CassandraDaemon.main(new String[0]);
		}
	}
	
}
