package com.thinkaurelius.titan.util.test;

import com.thinkaurelius.titan.configuration.CassandraNativeStorageConfiguration;
import com.thinkaurelius.titan.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.core.*;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EmbeddedCassandraExample {

	@Test
	public void basicNodeRetrieval() throws IOException {
		GraphDatabase gdb = startGraphDatabase(true);
		
		/* Populate graph database with Alice and Bob
		 * and one undirected edge connecting the two.
		 * 
		 * Then close the graph database and re-open it.
		 * 
		 * Then retrieve Alice and traverse the edge to Bob.
		 * 
		 * The goal of this test class is to demonstrate
		 * Titan using the Cassandra native API.  The tiny
		 * graph we load just demonstrates that the Titan
		 * instance resulting from this configuration can
		 * store and retrieve trivial data from Cassandra.
		 */
		GraphTransaction tx = gdb.startTransaction();
		
		// Create nodes and types
		Node alice = tx.createNode();
		Node bob = tx.createNode();
		RelationshipType related = tx.createEdgeType().withName("related").
				withDirectionality(Directionality.Undirected).
				category(EdgeCategory.Simple).makeRelationshipType();
		PropertyType name = tx.createEdgeType().withName("name").
				category(EdgeCategory.Simple).functional(true).
				makeKeyed().setIndex(PropertyIndex.Standard).
				dataType(String.class).makePropertyType();
		
		// Set props and relationships on nodes
		bob.createRelationship(related, alice);
		alice.createProperty(name, "Alice");
		bob.createProperty(name, "Bob");
		
		// Commit and close
		tx.commit();
		stopGraphDatabase(gdb);
		
		// Reopen database
		gdb = startGraphDatabase(false);
		tx = gdb.startTransaction();
		
		// Retrieve Alice and find Bob
		Set<Node> alices = tx.getNodesByAttribute("name", "Alice");
		
		assertEquals(1, alices.size());
		
		alice = alices.iterator().next();
		
		Iterable<Relationship> relatives = alice.getRelationships("related");
		Iterator<Relationship> relativeIter = relatives.iterator();
		assertTrue(relativeIter.hasNext());
		Relationship relative = relativeIter.next();
		
		Collection<? extends Node> nodes = relative.getNodes();
		assertEquals(2, nodes.size());
		assertTrue(nodes.contains(alice));
		
		Set<Node> bobSet = new HashSet<Node>(nodes);
		assertTrue(bobSet.remove(alice));
		bob = bobSet.iterator().next();
		
		assertEquals("Bob", bob.getAttribute("name"));
		
		// Close database
		tx.commit();
		stopGraphDatabase(gdb);
	}
	
	private GraphDatabase startGraphDatabase(boolean delete) throws IOException {
		File dogmaConfigDir = getTempDir();
		dogmaConfigDir.mkdirs();
		
		GraphDatabaseConfiguration cfg = 
				new GraphDatabaseConfiguration(dogmaConfigDir);

		CassandraNativeStorageConfiguration cassCfg =
				new CassandraNativeStorageConfiguration();

		cfg.setStorage(cassCfg);
		
		/*
		 * AbstractCassandraDaemon, a core part of Cassandra,
		 * contains a static initializer that tries to load
		 * a Log4j configuration file using a Log4j
		 * PropertyConfigurator object.  If the static initializer
		 * fails for any reason, it throws a RuntimeError which generally
		 * kills the entire JVM.  Below, we set up an empty file for 
		 * this static initializer to read, so that it won't throw a
		 * RuntimeError but also won't mess with our existing Log4j
		 * configuration.
		 * 
		 * We also set the cassandra.config system property below.
		 * Cassandra reads this property to find cassandra.yaml.
		 */
		
		// Create an empty file for Cassandra to "initialize" Log4j from
		File emptyLog4jConf = new File(dogmaConfigDir, "empty.properties");
		emptyLog4jConf.createNewFile();
		
		// Record current Log4j config property, if any
		String logProp = "log4j.configuration";
		String oldLog4jConf = System.getProperty(logProp);
		
		// Override Log4j config to empty file path
		System.setProperty(logProp, 
				"file://" + emptyLog4jConf.getCanonicalPath());
		
		// Set cassandra.yaml path
		System.setProperty("cassandra.config",
				"file:///tmp/casstest/cassandra.yaml");
		
		// Delete all Titan data
		// NOTE: this triggers Cassandra's Log4j static initializer
		if (delete)
			cassCfg.dropAllStorage();
		
		// Start graph database
		// NOTE: this triggers Cassandra's Log4j static initializer
		//       when !delete, so that the previous statement is passed over
		GraphDatabase gdb = cfg.openDatabase();

		// The Cassandra static initializer that cares about 
		// the log4j.configuration property has been executed.
		// Restore old Log4j config property, if any.
		// This step isn't necessary in a trivial example like
		// this one, but it doesn't hurt.
		if (null == oldLog4jConf)
			System.clearProperty(logProp);
		else
			System.setProperty(logProp, oldLog4jConf);
		
		return gdb;
	}
	
	private void stopGraphDatabase(GraphDatabase gdb) {
		gdb.close();
	}
	
	private File getTempDir() {
		return new File(System.getProperty("java.io.tmpdir"), 
				"titan-" + System.currentTimeMillis());
	}

}
