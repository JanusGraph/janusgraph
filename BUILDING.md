# Building JanusGraph

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

To build the distribution archive:

```
mvn clean install -Pjanusgraph-release -Dgpg.skip=true -DskipTests=true
```
This command generates the distribution archive in `janusgraph-dist/target/janusgraph-$VERSION.zip`.
For more details information, please see [here](janusgraph-dist/README.md#building-zip-archives)

## Building Docker Image for JanusGraph Server

In order to build Docker image for JanusGraph Server, a
distribution archive is needed. If you wish to build an image from source
refer to `To build the distribution archive` section to build the distribution
archive first. You can also use an [official release](https://github.com/JanusGraph/janusgraph/releases) to avoid building.
To do so check out the release tag you wish to build, example: `git checkout v0.2.0`. Then create target
directory that houses the distribution zip with `mkdir janusgraph-dist/target`.
The [downloaded release](https://github.com/JanusGraph/janusgraph/releases)
is then placed in the recently created target directory. Note that if the
tag is not found you can run `git fetch --all --tags --prune` and then rerun the checkout command.

Once the distribution is in place use the following command
to build and run Docker images with JanusGraph Server, configured
to run the BerkeleyJE backend and Elasticsearch (requires [Docker Compose](https://docs.docker.com/compose/)):

```bash
mvn docker:build -Pjanusgraph-docker -pl janusgraph-dist
docker-compose -f janusgraph-dist/docker-compose.yml up
```

If you are building the Docker image behind a proxy please set an environment variable for either http_proxy or https_proxy accordingly.

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

## Building documentation


### Updating documentation
You have check in the actual version of configuration reference. Therefore, you have to run following command:

```bash
mvn --quiet clean install -DskipTests=true -pl janusgraph-doc -am
```

### Required dependencies to build the documentation
MkDocs need to be installed to build and serve the documentation locally.

1. Install `python3` and `pip3` (newest version of pip) 
    * You can also checkout the installation guide of [material-mkdocs](https://squidfunk.github.io/mkdocs-material/getting-started/)
2. Install requirements using `pip3 install -r requirements.txt`

### Build and serve documentation

1. To create a test build locally use command `mkdocs build`
2. To serve the documentation locally use command `mkdocs serve`
