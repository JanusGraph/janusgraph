package com.thinkaurelius.titan.net.server;

import com.thinkaurelius.titan.configuration.CassandraStorageConfiguration;
import com.thinkaurelius.titan.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.GraphDB;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.net.Kernel;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * Basic main-class that starts a net.Kernel on Cassandra with test QueryTypes
 * registered.  For use as a remote Cassandra node in query testing. 
 * 
 * @author dalaro
 *
 */
public class Server {
	
	public static void main(String args[]) throws IOException {
		InetAddress localHost;
		try {
			localHost = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
		int port = Kernel.getDefaultListenPort();
		
		InetSocketAddress listen = new InetSocketAddress(localHost, port);
		
		String configDir;
		if (1 == args.length) {
			configDir = args[0];
		} else {
			configDir = ".";
		}
		GraphDatabaseConfiguration config = new GraphDatabaseConfiguration(configDir);
		CassandraStorageConfiguration cassConf = new CassandraStorageConfiguration();
		config.setStorage(cassConf);
		config.setReferenceNodeEnabled(true);
		GraphDB gdb = (GraphDB)config.openDatabase();
		Serializer ser = config.getSerializer();
		Kernel k = new Kernel(listen, gdb, ser, cassConf.getNodeIDMapper());
		
		k.registerQueryType(new ForwardTestQT());
		
		k.start();
		
		Runtime.getRuntime().addShutdownHook(new ShutdownHook(k, config));
		
		while (true) {
			try {
				Thread.sleep(100000L);
			} catch (InterruptedException e) {
				System.out.println("Caught InterruptedException");
				return;
			}
		}
	}
	
	private static class ShutdownHook extends Thread {
		private final Kernel kernel;
		private final GraphDatabaseConfiguration config;
		
		ShutdownHook(Kernel kernel, GraphDatabaseConfiguration config) {
			this.kernel = kernel;
			this.config = config;
		}
		
		@Override
		public void run() {
			System.out.println("Beginning graceful shutdown...");
			try {
				kernel.shutdown(5000L);
			} catch (InterruptedException e) {
				System.err.println("Exception while terminating Titan Kernel");
				e.printStackTrace();
			}
			config.close();
			System.out.println("Graceful shutdown complete.");
		}
	}
	
}
