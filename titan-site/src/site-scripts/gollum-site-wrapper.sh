#!/bin/bash

#
# Gollum uses libffi to call Pygments for syntax highlighting.
#
# On my box, Pygments sometimes nondeterministically segfaults the entire
# Ruby interpreter when I call the "gollum-site" tool to export our wiki.
#
# The segfault is not predictable, but it is reproducible: I can run the
# exact same gollum-site command in a loop, and sometimes is suceeds while
# sometimes it fails, with no obvious way to tell a priori.  Maybe there's
# an uninitialized pointer in there somewhere.
#
# Anyway, this script is a hack.  It runs gollum-site in a loop until
# it succeeds.  Maven can't really handle executables that fail and
# can't easily model "run this unreliable command in a loop until it
# eventually succeeds".  This script provides Maven with a script that
# will either return true eventually or, if gollum-site happens to
# always throw an error, never return.  The script does check that
# gollum-site is available on the path before going into its loop; if
# gollum-site is not available on the path, then the script dies right
# away with nonzero exit status.
#
# This whole script should die a quiet death if/when the gollum/Pygments
# problem gets sorted out.
#

att=1
exporter='gollum-site'

cd "$MAVEN{project.parent.basedir}/doc"

# Check for gollum-site command
type "$exporter" >/dev/null
[ 0 -eq $? ] || { echo "$0: $exporter not found, can't convert wiki to html."; exit -1; }

while [ 1 ] ; do
        echo "$0: $exporter execution attempt #${att}"
        rm -rf html
	"${exporter}" --base_path=./ --output_path=html/ generate 2>&1
        if [ 0 -eq $? ] ; then
                echo "$0: succeeded on execution attempt #${att}"
                break;
        else
                echo "$0: failed on execution attempt #${att}"
                sleep 1
                att=$(( $att + 1 ))
        fi
done
