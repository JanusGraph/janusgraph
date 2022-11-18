# Permanent stale index inconsistency

In some situations due to crashes of storage database, index database, or JanusGraph instances 
permanent stale index may appear.  
One of such cases could be a vertex removal from the graph but due to a crash during index persistence 
there is a chance that index won't be updated ever. 
In case we try to remove a vertex which is already gone from the storage database but which is still indexed then 
the `IllegalStateException` will be thrown with the message `Vertex with id %vertexId% was removed.`. 
For example, using `g.V().has("name", "HelloWorld").drop().iterate(); g.tx().commit();` we may receive an exception 
noting that some vertex has already been removed nevertheless it's record is still in the index. 
The exception will be thrown anytime we try to remove such vertex from the graph.  
This problem is known and should be temporal limitation until this issue is fixed in JanusGraph.  
As for now there is the utility tool which may be used to fix permanent stale indices.  

## StaleIndexRecordUtil

`StaleIndexRecordUtil.class` is available in `janusgraph-core` module and is meant to be used as a helper class 
to fix permanent stale index entries. 

`StaleIndexRecordUtil.forceRemoveVertexFromGraphIndex` can be used to force remove an index record for any vertex from 
a graph index.   
An example of using this method is below:
```java
// Let's say we want to remove non-existent vertex from a stale index. 
// We will assume the next constraints: 
// Vertex id is: `12345`
// Index name is: `nameAgeIndex`
// There are two indexed properties: `name` and `age`
// Value of the name property is: `HelloWorld`
// Value of the age property is: `123`

JanusGraph graph = JanusGraphFactory.open(configuration);

Map<String, Object> indexRecordPropertyValues = new HashMap<>();
indexRecordPropertyValues.put("name", "HelloWorld");
indexRecordPropertyValues.put("age", 123);

// After the below method is executed index entry of the vertex 12345 should be removed from the index which 
// effectively fixes permanent stale index inconsistency
StaleIndexRecordUtil.forceRemoveVertexFromGraphIndex(
    12345L, // vertex id of the index record to be removed
    indexRecordPropertyValues, // index record property values
    graph,
    "nameAgeIndex" // graph index name for which to remove the index record
);
```
For more in-depth information about usage of this tool as well as explanations of additional methods of this tool see JavaDoc for `StaleIndexRecordUtil`.
