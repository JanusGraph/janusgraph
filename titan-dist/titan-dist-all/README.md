
titan-dist-all
==============

Building zip/tar.bz2 archives
-----------------------------

Run `mvn clean install -Ptitan-release`.  Archives will be written to
the `target` folder.

Building deb/rpm packages
-------------------------

Requires:

* a platform that can run shell scripts (e.g. Linux, Mac OS X, or
  Windows with Cygwin).

* either the Aurelius public package GPG signing key or willingness to
  manually edit the package scripts to use your own package signing
  key

Run `mvn clean install -Ppkg-tools`.  This writes three folders to the
root of the titan repository:

* debian
* pkgcommon
* redhat

The debian and redhat folders contain platform-specific packaging
conttrol and payoad files.  The pkgcommon folder contains shared
payload and helper scripts.

To build packages, cd to the root of the titan repository,
i.e. `titan/titan-dist/titan-dist-all$ cd ../..`.  Then run
`pkgcommon/bin/build-all.sh`.
