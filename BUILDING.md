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

\* See note below for running tests with TinkerPop 3.2.3

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

## Running tests with TinkerPop 3.2.3

Several TinkerPop tests require commit `7aa9782`, which was merged after the release of TinkerPop 3.2.3. Tests requiring this commit include BulkDumperVertexProgramTest and PeerPressureTest. In order to run these tests TinkerPop must be locally patched and built as shown below. Note this is only necessary when running JanusGraph TinkerPop tests (`mvn clean install -Dtest.skip.tp=false`).

```bash
git clone https://github.com/apache/tinkerpop.git
cd tinkerpop
git checkout 3.2.3
git cherry-pick 7aa9782
mvn clean install -DskipTests -Drat.skip=true
```
