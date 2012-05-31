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
	
	public static synchronized void start() {
		if (started)
			return;
		
		log.debug("Current working directory: {}", System.getProperty("user.dir"));
		
		System.setProperty("cassandra.config",
				"file://" +
				System.getProperty("user.dir") + "/" + 
				"target/cassandra-tmp/conf/127.0.0.1/cassandra.yaml"); // TODO this is a hack
		System.setProperty("log4j.defaultInitOverride", "false");
		
		(new Thread(new CassandraRunner())).run();
		
		started = true;
	}
	
	private static class CassandraRunner implements Runnable {
		@Override
		public void run() {
			CassandraDaemon.main(new String[0]);
		}
	}
	
}
