#!/bin/bash

set -e
set -u

# Change to titan repository root
cd "`dirname $0`/../../"

# Load config
. pkgcommon/etc/config.sh

# Clean dependency dir
echo Deleting contents of "$DEP_DIR"...
find $DEP_DIR -mindepth 1 -maxdepth 1 -exec rm -rf '{}' \;
