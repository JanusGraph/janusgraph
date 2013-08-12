#!/bin/bash

#
# Partition Titan's dependency set to support modular packaging.
#
# Dependencies used in more than one module ("used" directly or
# indirectly through an arbitrary degree of transitivity) go into a
# partition cell called main.  Dependencies that appear in exactly one
# module go into a cell named after the module.  The list of
# dependencies in each cell are written to the files
# dep-<cellname>.txt in the repo root.
#
# The point of all this is to determine which jars should go into
# which .deb or .rpm subpackage.  I don't think Maven's dependency or
# assembly plugins are currently capable of doing this computation
# natively, so I wrote this script.
#

set -e
set -u

# Change to titan repository root
cd "`dirname $0`/../../"

# Load config
. pkgcommon/config.sh.in

# Clean up
rm -f $DEP_DIR/{dep,jar}-main.txt
for m in $MODULES; do
    rm -f $DEP_DIR/{dep,jar}-$m.txt
done

# Generate complete dependency list (incl transitive deps) for each module
for m in $MODULES; do
    cd titan-$m
    mvn dependency:list $MVN_OPTS \
        -DexcludeGroupIds=com.thinkaurelius.titan \
        -DoutputFile=$DEP_DIR/dep-$m.txt -DincludeScope=runtime \
        -DoutputAbsoluteArtifactFilename=true
    # Delete first two lines of human-readable junk and kill leading spaces
    sed -ri '1d; 2d; s/^ +//; /^$/d' $DEP_DIR/dep-$m.txt
    cat $DEP_DIR/dep-$m.txt | sort > $DEP_DIR/dep-$m.txt.tmp
    mv  $DEP_DIR/dep-$m.txt{.tmp,}
    cd - >/dev/null
done

# Any dependency that appears in more than one module goes in dep-main.txt
cat $DEP_DIR/dep-*.txt | sort | uniq --repeated > $DEP_DIR/dep-main.txt

# Any dependency that appears in exactly one module goes in dep-<module>.txt
for m in $MODULES; do
    comm -1 -3 $DEP_DIR/dep-main.txt $DEP_DIR/dep-$m.txt > $DEP_DIR/dep-$m.txt.tmp
    mv $DEP_DIR/dep-$m.txt{.tmp,}
done


# This is a hack for Lucene and ES.  Every Lucene jar used by titan-lucene
# is also used by titan-es, so those go into titan-main.  Some additional
# Lucene jars are used only by titan-es, so those go into titan-es.  The
# result is a completely empty dep-lucene.txt file with Lucene jars split
# between dep-es.txt and dep-main.txt.  It's technically the intended
# result of the partition algorithm, but it's also utterly nonsensical from
# a practical point of view.
#
# Force all org.apache.lucene packages into dep-lucene.txt.  The .deb and
# .rpm package definitions must make the titan-es subpackage depend on the
# titan-lucene subpackage for this to work.
for m in $MODULES main; do
    [ "$m" = "lucene" ] && continue
    grep '^org\.apache\.lucene:' $DEP_DIR/dep-$m.txt >> $DEP_DIR/dep-lucene.txt || continue
    grep -v '^org\.apache\.lucene:' $DEP_DIR/dep-$m.txt >> $DEP_DIR/dep-$m.txt.tmp
    mv $DEP_DIR/dep-$m.txt{.tmp,}
done

cat $DEP_DIR/dep-lucene.txt | sort > $DEP_DIR/dep-lucene.txt.tmp
mv $DEP_DIR/dep-lucene.txt{.tmp,}

# Convert dependency lists to equivalent jar filename lists
for m in $MODULES main; do
    for j in `cat $DEP_DIR/dep-$m.txt | sed -r 's/(.+):(.+)/\2/'`; do
        echo $j >> $DEP_DIR/jar-$m.txt
    done
done

