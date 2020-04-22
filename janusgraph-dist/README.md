# janusgraph-dist

## Building zip archives

Run `mvn clean install -Pjanusgraph-release -Dgpg.skip=true
-DskipTests=true`.  This command can be run from either the root of
the JanusGraph repository (the parent of the janusgraph-dist directory) or the
janusgraph-dist directory.  Running from the root of the repository is
recommended.  Running from janusgraph-dist requires that JanusGraph's jars be
available on either Sonatype, Maven Central, or your local Maven
repository (~/.m2/repository/) depending on whether you're building a
SNAPSHOT or a release tag.

This command writes one archive:

* janusgraph-dist/target/janusgraph-$VERSION.zip

It's also possible to leave off the `-DskipTests=true`.  However, in
the absence of `-DskipTests=true`, the -Pjanusgraph-release argument
causes janusgraph-dist to run several automated integration tests of the
zipfiles and the script files they contain.  These tests require unzip
and expect, and they'll start and stop Cassandra, ES, and HBase in the
course of their execution.

## Building documentation

To convert the markdown sources in $JANUSGRAPH_REPO_ROOT/docs/ to chunked, 
run `mvn install -DskipTests=true -pl janusgraph-doc -am` and `mkdocs build`.

The documentation output appears in:

* site/

## Building deb/rpm packages

Requires:

* a platform that can run shell scripts (e.g. Linux, Mac OS X, or
  Windows with Cygwin)

* the Aurelius public package GPG signing key

Run `mvn -N -Ppkg-tools install` in the janusgraph-dist module.  This writes
three folders to the root of the janusgraph repository:

* debian
* pkgcommon
* redhat

The debian and redhat folders contain platform-specific packaging
control and payload files.  The pkgcommon folder contains shared
payload and helper scripts.

To build the .deb and .rpm packages:

* (cd to the repository root)
* `pkgcommon/bin/build-all.sh`

To delete the packaging scripts from the root of the repository, run
`mvn -N -Ppkg-tools clean` from the janusgraph-dist module.

## Upgrade cassandra-server version 

Following files have to be updated, if you update Cassandra server version 
in the default JanusGraph distribution:

  * src/assembly/static/cassandra/bin/cassandra
  * src/assembly/static/cassandra/conf/cassandra.yaml
  * src/assembly/static/cassandra/conf/jvm.options
  * src/assembly/static/cassandra/conf/logback.xml

These files contains just small changes which allows us to include Cassandra 
into our distribution. Changed locations are marked using 
`JanusGraph change:`.
