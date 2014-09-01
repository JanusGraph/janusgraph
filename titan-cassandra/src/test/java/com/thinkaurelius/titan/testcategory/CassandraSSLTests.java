package com.thinkaurelius.titan.testcategory;

/**
 * This is a JUnit category for tests that need to run against Cassandra
 * configured for SSL-based client authentication.
 *
 * If you rename or move this class, then you must also update mentions of it in
 * the Cassandra module's pom.xml.
 */
public interface CassandraSSLTests { }
