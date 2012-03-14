
package com.thinkaurelius.titan.configuration;

import com.thinkaurelius.titan.core.GraphDatabase;
import com.thinkaurelius.titan.core.GraphTransaction;
import com.thinkaurelius.titan.graphdb.transaction.InMemoryGraphDB;

/***
 * Opens in-memory graph databases and transactions.
 * This class serves as a factory to open in-memory databases and transactions. It cannot be instantiated or extended.
 * 
 * In-memory graph databases are graph databases which are not persisted to disk and are kept entirely in memory.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public final class InMemoryGraphDatabase {

	private InMemoryGraphDatabase() {};
	
	 /**
	  * Opens and returns an in-memory graph database.
	  * 
	  * An in-memory graph database does not persist any data to disk but keeps it entirely in memory.
	  * Note that in-memory graph databases do not support multiple transactions. If multiple in-memory
	  * transactions are needed, open multiple in-memory databases.
	  * 
	  * @return An in-memory graph database
	  * @see GraphDatabase
	  */
	 public static GraphDatabase open() {
		 return new InMemoryGraphDB(true);
	 }
	 
	 /**
	  * Opens and returns an in-memory graph transaction.
	  * 
	  * An in-memory graph transaction does not persist any data to disk but keeps it entirely in memory.
	  * 
	  * @return An in-memory graph transaction
	  * @see GraphTransaction
	  */
	 public static GraphTransaction startTransaction() {
		 return new InMemoryGraphDB(true);
	 }
	
}
