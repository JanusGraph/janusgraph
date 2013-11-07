Testing Titan
=============

### Audience of this Document

This page is written for developers familiar with Java, Titan, and Maven who want information on how to run Titan's test suite.  Users interested in installing and running Titan releases should consult the [wiki](https://github.com/thinkaurelius/titan/wiki).

### Overview

Titan runs all tests using JUnit.  To compile, package, and run the default test suite for Titan, use the standard `mvn clean install` command.

Titan has a specialty tests, disabled by default, intended to generate basic performance metrics or stress its cache structures under memory pressure.  The next section describes how Titan's tests are internally categorized and the Maven options that enabled/disable test categories.

### JUnit Test Categories

All of Titan's tests are written for JUnit.  Titan's JUnit tests are annotated with the following [JUnit Categories](https://github.com/junit-team/junit/wiki/Categories):


| Category Name | Maven Property | Default | Comment |
| ------------- | ------------------- |:------------:| ------- |
| MemoryTests | test.skip.mem | true (disabled) | Tests intended to exert memory pressure |
| PerformanceTests | test.skip.perf | true (disabled) | Tests written as simple speed tests using JUnitBenchmarks|
| OrderedKeyStoreTests | test.skip.ordered | false (enabled) | Tests written for a storage backend that stores data in key order |
| UnorderedKeyStoreTests | test.skip.unordered | false (enabled) | Tests written for a storage backend that doesn't store data in key order |
| (No&nbsp;category) | test.skip.default | false (enabled) | Tests without any Category annotations |

**Category Name** above is a Java interface defined in the package [com.thinkaurelius.titan.testcategory](titan-test/src/main/com/thinkaurelius/titan/testcategory).  These interfaces appear as arguments to the JUnit `@Category(...)` annotation, e.g. `@Category({MemoryTests.class})`.

**Maven Property** above is a boolean-valued pom.xml property that skips the associated test category when true and executes the associated test category when false.  The default values defined in pom.xml can be overridden on the command-line in the ordinary Maven way, e.g. `mvn -Dtest.skip.mem=false test`.

*Implementation Note.*  The Maven property naming pattern "test.skip.x=boolean" is needlessly verbose, a cardinal sin for command line options.  A more concise alternative would be "test.x" with the boolean sense negated.  However, this complicates the pom.xml configuration for the Surefire plugin since it precludes direct use of the Surefire plugin's `<skip>` configuration tag, as in `<skip>${test.skip.perf}</skip>`.  There doesn't seem to be a straightforward way to negate a boolean or otherwise make this easy, at least without resorting to profiles or a custom plugin, though I might be missing something.  Also, the mold is arguably already set by Surefire's "maven.test.skip" property, though that has slightly different interpretation semantics than the properties above.    

### Running a Single Test via Maven

The standard maven-surefire-plugin option applies for most tests:

```bash
mvn test -Dtest=full.or.partial.classname#methodname
```

However, MemoryTests and PerformanceTests are disabled by default regardless of whatever `-Dtest=...` option might be specified.  When running a single MemoryTest or PerformanceTest, specify `-Dtest.mem=true` or `-Dtest.perf=true` as appropriate for the test in question.

Here's a concrete example.

```bash
# Executes no tests because the MemoryTests category is disabled by default
mvn test -Dtest=BerkeleyJEGraphPerformanceMemoryTest
# Executes the specified test
mvn test -Dtest=BerkeleyJEGraphPerformanceMemoryTest -Dtest.skip.mem=false
```
