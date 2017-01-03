#!/bin/bash

#
# Partition Janus's dependency set to support modular packaging.
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

# Change to janus repository root
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
        -Paurelius-release \
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
    cd janus-$m
    mvn_deps "$DEP_DIR/dep-part-$m.txt"
    cd - >/dev/null
done

# dep-all.txt lists every dependency in the project
cd janus-dist/janus-dist-all/
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

# We can't use dep-repeated.txt as the contents of the main janus
# partition, because that would drop jars that exist only on the
# janus-dist assembly.  Generate the main janus partition my
# subtracting the union of all module partitions from dep-all.txt.
cat $DEP_DIR/dep-part-*.txt | sort | uniq > $DEP_DIR/dep-modules.txt
comm -2 -3 $DEP_DIR/dep-all.txt $DEP_DIR/dep-modules.txt > $DEP_DIR/dep-part-main.txt


# This is a hack for Lucene and ES.  Every Lucene jar used by janus-lucene
# is also used by janus-es, so those go into janus-main.  Some additional
# Lucene jars are used only by janus-es, so those go into janus-es.  The
# result is a completely empty dep-lucene.txt file with Lucene jars split
# between dep-es.txt and dep-part-main.txt.  It's technically the intended
# result of the partition algorithm, but it's also utterly nonsensical from
# a practical point of view.
#
# Force all org.apache.lucene packages into dep-lucene.txt.  The .deb and
# .rpm package definitions must make the janus-es subpackage depend on the
# janus-lucene subpackage for this to work.
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
	echo "$my_pwd"/janus-core/target/janus-core-$VER.jar >> $DEP_DIR/jar-$m.txt
    else
	echo "$my_pwd"/janus-$m/target/janus-$m-$VER.jar >> $DEP_DIR/jar-$m.txt
    fi
done
