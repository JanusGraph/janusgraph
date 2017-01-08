#!/bin/bash

# The release integration tests download JanusGraph database files
# created by several past releases, then attempt to open and do
# some simple read and write operations on those files.  This is
# a simple compatibility test.  It's independent of the
# property janusgraph.compatible.versions in the top-level pom.xml;
# it uses a separate file in janusgraph-dist called compat.csv that
# lists versions and URLs from which tarballs containing DB files
# will be downloaded for testing.
#
# I've tried to automate this step into release:prepare, but the
# plugin does not support adding files to the commits that it
# creates.  It expliticly names all of the poms in its invocation
# of `git commit`, so other files added to the git index won't be
# committed.  There's no way to override or append to the list of
# committed files.  The issue to remove this limitation,
# https://jira.codehaus.org/browse/MRELEASE-798, has
# been open for years.  The maintainers reject the approach taken
# in the PR and no alternative is forthcoming.  So, I would have
# to either embed the compat.csv information in one of the poms
# (maven-release-plugin does commit poms), or resort to adding
# another release step.  I went with the latter.

set -e
set -u

echo Reading settings from $1
declare -r SCM_TAG=`sed -rn 's|\\\\||g; s|^scm\.tag=||p' $1`

COMPAT_MANIFEST='janusgraph-dist/src/test/resources/compat.csv'
COMPAT_CONFIG='bdb-es'
COMPAT_TGZ_URL="https://s3.amazonaws.com/titan-release-compat-test/${COMPAT_CONFIG}/${SCM_TAG}.zip"

if grep "^${SCM_TAG},${COMPAT_CONFIG}," "$COMPAT_MANIFEST" >/dev/null ; then
    echo "Compatibility test manifest file at path:"
    echo "   $COMPAT_MANIFEST"
    echo "already contains an entry for version ${SCM_TAG} (${COMPAT_CONFIG})."
    echo "Leaving manifest unmodified and exiting."
    exit 0
fi

echo "${SCM_TAG},${COMPAT_CONFIG},$COMPAT_TGZ_URL" >> "$COMPAT_MANIFEST"

git add "$COMPAT_MANIFEST"

git commit -m "Add ${SCM_TAG} to compat manifest" "$COMPAT_MANIFEST"
