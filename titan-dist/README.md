titan-dist
==========

Building zip archives
-----------------------------

Run `mvn clean install -Paurelius-release -Dgpg.skip=true
-DskipTests=true`.  This command can be run from either the root of
the Titan repository (the parent of the titan-dist directory) or the
titan-dist directory.  Running from the root of the repository is
recommended.  Running from titan-dist requries that Titan's jars be
available on either Sonatype, Maven Central, or your local Maven
repository (~/.m2/repository/) depending on whether you're building a
SNAPSHOT or a release tag.

This command writes two archives:

* titan-dist/titan-dist-hadoop-1/target/titan-$VERSION-hadoop1.zip
* titan-dist/titan-dist-hadoop-2/target/titan-$VERSION-hadoop2.zip

It's also possible to leave off the `-DskipTests=true`.  However, in
the absence of `-DskipTests=true`, the -Paurelius-release argument
causes titan-dist to run several automated integration tests of the
zipfiles and the script files they contain.  These tests require unzip
and expect, and they'll start and stop Cassandra, ES, and HBase in the
course of their execution.

Building documentation
----------------------

To convert the AsciiDoc sources in $TITAN_REPO_ROOT/docs/ to chunked
and single-page HTML, run `mvn package -pl titan-dist -am
-DskipTests=true -Dgpg.skip=true` from the directory containing
titan-dist.  If the Titan artifacts are already installed in the local
Maven repo from previous invocations of `mvn install` in the root of
the clone, then `cd titan-dist && mvn package` is also sufficient.

The documentation output appears in:

* titan-dist/target/docs/chunk/
* titan-dist/target/docs/single/

Building deb/rpm packages
-------------------------

Requires:

* a platform that can run shell scripts (e.g. Linux, Mac OS X, or
  Windows with Cygwin)

* the Aurelius public package GPG signing key

Run `mvn -N -Ppkg-tools install` in the titan-dist module.  This writes
three folders to the root of the titan repository:

* debian
* pkgcommon
* redhat

The debian and redhat folders contain platform-specific packaging
conttrol and payoad files.  The pkgcommon folder contains shared
payload and helper scripts.

To build the .deb and .rpm packages:

* (cd to the repository root)
* `pkgcommon/bin/build-all.sh`

To delete the packaging scripts from the root of the repository, run
`mvn -N -Ppkg-tools clean` from the titan-dist module.

Gollum-site is no longer required
---------------------------------

Previous versions of titan-dist needed a companion module called
titan-site, which in turn required the gollum-site binary to be
command on the local system.  This is no longer required now that the
docs have moved from the GitHub wiki to AsciiDoc files stored in the
repo.  The AsciiDoc files are converted to HTML using a DocBook-based
toolchain completely managed by maven.
