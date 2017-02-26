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

To build with default plus TinkerPop tests\*:

```
mvn clean install -Dtest.skip.tp=false
```

To build with only the TinkerPop tests\*:

```
mvn clean install -Dtest.skip.tp=false -DskipTests=true
```

To build and run a docker image with JanusGraph and Gremlin Server, configured
to run the berkeleyje backend with an embedded ElasticSearch instance:

```bash
mvn clean package -Pjanusgraph-release -Dgpg.skip=true -DskipTests=true
cd janusgraph-hbase-parent/janusgraph-hbase-core && mvn install -DskipTests=true && cd ../..
cd janusgraph-dist && mvn install -Pjanusgraph-docker -DskipTests=true docker:build
docker run -d -p 8182:8182 --name janusgraph janusgraph/server:latest
```

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

