#!/bin/bash
# Copyright 2019 JanusGraph Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License

#
# Partition JanusGraph's dependency set to support modular packaging.
#
# Dependencies used in more than one module ("used" directly or
# indirectly through an arbitrary degree of transitivity) go into a
# partition cell called main.  Dependencies that appear in exactly one
# module go into a cell named after the module.  The list of
# dependencies in each cell are written to the files
# dep-part-<cellname>.txt in the repo root.
#
# The point of all this is to determine which jars should go into
# which .deb or .rpm subpackage.  I don't think Maven's dependency or
# assembly plugins are currently capable of doing this computation
# natively, so I wrote this script.
#

set -e
set -u

# Change to janusgraph repository root
cd "`dirname $0`/../../"

# Load config
. pkgcommon/etc/config.sh
. pkgcommon/etc/version.sh

# Clean up
pkgcommon/bin/clean.sh

#set -x

mvn_deps() {
    local outfile="$1"
    mvn dependency:list $MVN_OPTS \
        -Pjanusgraph-release \
        -DexcludeGroupIds=org.janusgraph \
        -DoutputFile=$outfile -DincludeScope=runtime \
        -DoutputAbsoluteArtifactFilename=true
    # Delete first two lines of human-readable junk and kill leading spaces
    sed -ri '1d; 2d; s/^ +//; /^$/d' $outfile
    cat $outfile | sort > $outfile.tmp
    mv  $outfile{.tmp,}
}

# Generate complete dependency list (incl transitive deps) for each module
for m in $MODULES; do
    cd janusgraph-$m
    mvn_deps "$DEP_DIR/dep-part-$m.txt"
    cd - >/dev/null
done

# dep-all.txt lists every dependency in the project
cd janusgraph-dist/janusgraph-dist-all/
mvn_deps "$DEP_DIR/dep-all.txt"
cd - >/dev/null
cat $DEP_DIR/dep-part-*.txt $DEP_DIR/dep-all.txt | sort | uniq > \
    $DEP_DIR/dep-all.txt.tmp
mv  $DEP_DIR/dep-all.txt{.tmp,}

# dep-repeated.txt lists all deps that occur in two or more modules
cat $DEP_DIR/dep-part-*.txt | sort | uniq --repeated > $DEP_DIR/dep-repeated.txt

# Rewrite dep-<module>.txt files: Any dependency that appears in
# exactly one module goes in dep-<module>.txt; remaining deps are
# deleted.
for m in $MODULES; do
    comm -1 -3 $DEP_DIR/dep-repeated.txt $DEP_DIR/dep-part-$m.txt > $DEP_DIR/dep-part-$m.txt.tmp
    mv $DEP_DIR/dep-part-$m.txt{.tmp,}
done

# We can't use dep-repeated.txt as the contents of the main janusgraph
# partition, because that would drop jars that exist only on the
# janusgraph-dist assembly.  Generate the main janusgraph partition my
# subtracting the union of all module partitions from dep-all.txt.
cat $DEP_DIR/dep-part-*.txt | sort | uniq > $DEP_DIR/dep-modules.txt
comm -2 -3 $DEP_DIR/dep-all.txt $DEP_DIR/dep-modules.txt > $DEP_DIR/dep-part-main.txt


# This is a hack for Lucene and ES.  Every Lucene jar used by janusgraph-lucene
# is also used by janusgraph-es, so those go into janusgraph-main.  Some additional
# Lucene jars are used only by janusgraph-es, so those go into janusgraph-es.  The
# result is a completely empty dep-lucene.txt file with Lucene jars split
# between dep-es.txt and dep-part-main.txt.  It's technically the intended
# result of the partition algorithm, but it's also utterly nonsensical from
# a practical point of view.
#
# Force all org.apache.lucene packages into dep-lucene.txt.  The .deb and
# .rpm package definitions must make the janusgraph-es subpackage depend on the
# janusgraph-lucene subpackage for this to work.
for m in $MODULES main; do
    [ "$m" = "lucene" ] && continue
    grep '^org\.apache\.lucene:' $DEP_DIR/dep-part-$m.txt >> $DEP_DIR/dep-part-lucene.txt || continue
    grep -v '^org\.apache\.lucene:' $DEP_DIR/dep-part-$m.txt >> $DEP_DIR/dep-part-$m.txt.tmp
    mv $DEP_DIR/dep-part-$m.txt{.tmp,}
done
cat $DEP_DIR/dep-part-lucene.txt | sort > $DEP_DIR/dep-part-lucene.txt.tmp
mv  $DEP_DIR/dep-part-lucene.txt{.tmp,}


my_pwd=`pwd`

# Convert dependency lists to equivalent jar filename lists
for m in $MODULES main; do
    for j in `cat $DEP_DIR/dep-part-$m.txt | sed -r 's/(.+):(.+)/\2/'`; do
        echo $j >> $DEP_DIR/jar-$m.txt
    done
    if [ 'main' = $m ]; then
	echo "$my_pwd"/janusgraph-core/target/janusgraph-core-$VER.jar >> $DEP_DIR/jar-$m.txt
    else
	echo "$my_pwd"/janusgraph-$m/target/janusgraph-$m-$VER.jar >> $DEP_DIR/jar-$m.txt
    fi
done
