#!/bin/bash

# Deploys distro archives and stages JARs to Nexus.
#
# Does not push anything to GitHub or release the Nexus repo.

set -u
set -e

declare -r WORK_DIR="/tmp/checkout"
declare -r CMD='mvn clean deploy -Paurelius-release'

echo Rsyncing current directory to $WORK_DIR...
rm -rf "$WORK_DIR"
rsync -qav . "$WORK_DIR"
echo Changing current directory to $WORK_DIR...
cd "$WORK_DIR"

echo Reading settings from $1
declare -r SCM_TAG=`sed -rn 's|\\\\||g; s|^scm\.tag=||p' $1`
declare -r TAG_REF=refs/tags/"$SCM_TAG"
echo Read SCM_TAG: "$SCM_TAG"

echo Checking out $TAG_REF...
git checkout "$TAG_REF"

echo Executing $CMD...
$CMD
