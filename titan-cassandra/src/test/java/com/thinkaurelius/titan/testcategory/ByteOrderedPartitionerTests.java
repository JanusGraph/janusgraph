package com.thinkaurelius.titan.testcategory;

/**
 * This is a JUnit category for tests that need a Cassandra server with a
 * byte-ordered partitioner configured.
 * 
 * If you rename or move this class, then you must also update mentions of it in
 * the Cassandra module's pom.xml.
 * 
 * @see RandomPartitionerTests
 */
public class ByteOrderedPartitionerTests { }
