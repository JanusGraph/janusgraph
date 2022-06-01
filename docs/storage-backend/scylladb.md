# ScyllaDB

![](scylladb.svg)

> ScyllaDB is a NoSQL database with a close-to-the-hardware, shared-nothing approach that optimizes raw performance, fully utilizes modern multi-core servers, and minimizes the overhead to DevOps. ScyllaDB is API-compatible with both Cassandra and DynamoDB, yet is much faster, more consistent, and with a lower TCO.
>
> —  [ScyllaDB
> Homepage](https://www.scylladb.com/)

## ScyllaDB Setup and Connection

ScyllaDB is fully compatible with Cassandra. To use it as the data storage layer:

1. Spin up a Scylla cluster. You can do this using Scylla Cloud, using Docker, running Scylla in the cloud or on-prem. You can see a step-by-step guide on how to [spin up a three node Scylla cluster using Docker](https://university.scylladb.com/courses/scylla-essentials-overview/lessons/high-availability/topic/consistency-level-demo-part-1/) in this lesson.
2. Run JanusGraph with “cql” as the storage.backend. 
3. Specify the IP address of one of the Scylla nodes in your cluster as the storage.hostname.


### Step by Step Tutorial

[This Scylla University lesson provides step-by-step instructions for using JanusGraph with ScyllaDB](https://university.scylladb.com/courses/the-mutant-monitoring-system-training-course/lessons/a-graph-data-system-powered-by-scylladb-and-janusgraph/) as the data storage layer. The main steps in the lesson are:

- Spinning up a virtual machine 
- Installing the prerequisites
- Running the JanusGraph server (using Docker)
- Running a Gremlin Console to connect to the new server (also in Docker)
- Spinning up a three-node Scylla Cluster and setting it as the data storage for the JanusGraph server
- Performing some basic graph operations

