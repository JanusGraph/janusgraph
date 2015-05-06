Building Titan
--------------

Required:

* Java 7 (0.5 and earlier) or Java 8 (0.9 and later)
* Maven

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
