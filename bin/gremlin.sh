#!/bin/bash
cmd_safe="mvn install"
cmd_unsafe="mvn install -DskipTests=true"
echo "Titan must be built before running Gremlin." >&2
echo "Run \"${cmd_safe}\" in the repository root to build and test Titan." >&2
echo "Alternatively, run \"${cmd_unsafe}\" in the repository root to build but not test Titan." >&2
echo -n "Do you want to execute \"$cmd_safe\" now? [Y/n] "
read response
if [ "y" = "$response" -o "Y" = "$response" -o "" = "$response" ] ; then
    cd "`dirname $0`"/../
    $cmd_safe
fi
