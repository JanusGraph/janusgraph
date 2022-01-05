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

We moved the docker build into an external repo: https://github.com/JanusGraph/janusgraph-docker.

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
