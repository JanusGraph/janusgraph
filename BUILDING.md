Building JanusGraph
--------------

Required:

* Java 8
* Maven 3

To build without executing tests:

```
mvn clean install -DskipTests=true
```

To build with default tests:

```
mvn clean install
```

To build with default plus TinkerPop tests:

```
mvn clean install -Dtest.skip.tp=false
```

To build with only the TinkerPop tests:

```
mvn clean install -Dtest.skip.tp=false -DskipTests=true
```

## Building Docker Image for JanusGraph Gremlin Server

To build and run Docker images with JanusGraph and Gremlin Server, configured 
to run the BerkeleyJE backend and Elasticsearch (requires [Docker Compose](https://docs.docker.com/compose/)):

```bash
mvn clean install -Pjanusgraph-release -Dgpg.skip=true -DskipTests=true && mvn docker:build -Pjanusgraph-docker -pl janusgraph-dist
docker-compose -f janusgraph-dist/janusgraph-dist-hadoop-2/docker-compose.yml up
```

Note the above `docker-compose` call launches containers in the foreground and is convenient for monitoring logs but add "-d" to instead run in the background.

To connect to the server in the same container on the console:

```bash
docker exec -i -t janusgraph /var/janusgraph/bin/gremlin.sh
```

Then you can interact with the graph on the console through the `:remote` interface:

```groovy
gremlin> :remote connect tinkerpop.server conf/remote.yaml
==>Configured localhost/127.0.0.1:8182
gremlin> :remote console
==>All scripts will now be sent to Gremlin Server - [localhost/127.0.0.1:8182] - type ':remote console' to return to local mode
gremlin> GraphOfTheGodsFactory.load(graph)
==>null
gremlin> g = graph.traversal()
==>graphtraversalsource[standardjanusgraph[berkeleyje:db/berkeley], standard]
```

## Building on Eclipse IDE
Note that this has only been tested on Eclipse Neon.2 Release (4.6.2) with m2e (1.7.0.20160603-1933) and m2e-wtp (1.3.1.20160831-1005) plugin.


To build without executing tests:

1. Right-click on your project -> "Run As..." -> "Run Configurations..."
2. On "Goals", populate with `install`
3. Select the options `Update Snapshots` and `Skip Tests`
4. Before clicking "Run", make sure that Eclipse knows where `JAVA_HOME` is. On same window, go to "Environment" tab and click "New".
5. Under "Name:", add `JAVA_HOME`
6. Under "Value:", add the path where `java` is located
7. Click "OK"
8. Then click "Run"

To find the Java binary in your environment, run the appropriate command for your operating system:
* Linux/macOS: `which java`
* Windows: `for %i in (java.exe) do @echo. %~$PATH:i`

