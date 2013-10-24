
titan-dist
==========

Building zip/tar.bz2 archives
-----------------------------

Run `mvn clean install -Paurelius-release -Dgpg.skip=true`.  Archives
files will be written to `titan-dist-*/target/`.  The archives expect
that titan-site has already been installed locally with the same 
command (but executed in ../titan-site).  titan-site requires the
gollum-site utility to export a copy of the wikidocs which is later
included by the distribution archives.  If you don't have gollum-site
installed, then consult the workaround here:

https://github.com/thinkaurelius/titan/wiki/Building-Titan#building-distributions-without-gollum

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
