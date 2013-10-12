
titan-dist-all
==============

Building zip/tar.bz2 archives
-----------------------------

Run `mvn clean install -Paurelius-release -Dgpg.skip=true`.  Archives
files will be written to `titan-dist-*/target/`. Note that if not
generating wiki docs with gollum it is necessary to comment out
titan-site in the top-level pom.xml and the dependency in
titan-dist.pom.xml

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
