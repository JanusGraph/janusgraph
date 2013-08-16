#!/bin/bash

set -e
set -u

declare -r DN="`dirname $0`"

"$DN"/../../debian/build.sh
"$DN"/../../redhat/build.sh
