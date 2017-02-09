#!/bin/bash

#
# Update gh-pages during a release.
#
# Usage: gh-pages-update.sh release.properties
#

set -e
set -u

# Set some path constants
declare -r PAGE_BRANCH=gh-pages
declare -r MAIN_INDEX=$MAVEN{project.build.directory}/release/index.html
#declare -r HTDOC_DIR=$MAVEN{htmlchunk.output.dir}
declare -r JAVADOC_DIR=$MAVEN{project.build.directory}/javadocs
declare -r CLONE_DIR=/tmp/janusgraphpages

if [ -z "${GIT_DIR:-}" -o -z "${GIT_TAG:-}" ]; then
    if [ -z "${1:-}" ]; then
        echo "Usage: $0 path/to/release.properties"
        exit -1
    else
        # Change directory to the folder containing release.properties,
        # store the full path to that directory, then clone it to $CLONE_DIR.
        pushd "`dirname $1`" >/dev/null
        declare -r GIT_DIR="`pwd`"
        popd >/dev/null
        echo Read GIT_DIR from $1: $GIT_DIR

        # Search release.properties for the current release tag.
        declare -r GIT_TAG=`sed -rn 's|\\\\||g; s|^scm\.tag=||p' $1`
        echo Read GIT_TAG from $1: $GIT_TAG
    fi
else
    echo Read GIT_DIR from environment: $GIT_DIR
    echo Read GIT_TAG from environment: $GIT_TAG
fi

# Create gh-pages branch in the source repo if it doesn't exist.
# This command assumes the github remote is named "origin".
git show-ref --verify --quiet refs/heads/"$PAGE_BRANCH"
if [ $? -ne 0 ]; then
    git branch "$PAGE_BRANCH" origin/"$PAGE_BRANCH"
fi

# Clone repo, change into its directory, checkout gh-pages branch
# These commands also assume the github remote is named "origin".
echo Cloning $GIT_DIR
git clone "$GIT_DIR" "$CLONE_DIR"
cd "$CLONE_DIR"
echo Checking out $PAGE_BRANCH
git fetch origin refs/remotes/origin/"$PAGE_BRANCH":refs/heads/"$PAGE_BRANCH"
git checkout "$PAGE_BRANCH"
git pull origin "$PAGE_BRANCH"

## Wikidoc is an anachronism (we've moved to AsciiDoc)
#echo Copying chunked htmldocs to wikidoc/$GIT_TAG
#cd "$CLONE_DIR"/wikidoc
#cp -a "$HTDOC_DIR" "$GIT_TAG"
#echo Making wikidoc/current a copy of wikidoc/$GIT_TAG
#git rm -qr current
#cp -a   "$GIT_TAG" current
#echo Adding wikidoc changes to git index
#git add "$GIT_TAG" current

echo Copying javadoc to javadoc/$GIT_TAG
cd "$CLONE_DIR"/javadoc
cp -a "$JAVADOC_DIR" "$GIT_TAG"
echo Making javadoc/current a copy of javadoc/$GIT_TAG
git rm -qr current
cp -a   "$GIT_TAG" current
echo Adding javadoc changes to git index
git add "$GIT_TAG" current

echo Copying new gh-pages index.html to root
cd $CLONE_DIR
cp "$MAIN_INDEX" index.html
echo Adding new gh-pages index.html file to the git index
git add index.html

git commit -m "Page updates for $GIT_TAG"

cat <<EOF
Locally committed javadoc dirs (current and $GIT_TAG) and index.html
for release version $GIT_TAG on branch gh-pages.

  Clone directory = $CLONE_DIR
  Parent repo     = $GIT_DIR
  Branch modified = $PAGE_BRANCH
  Release version = $GIT_TAG

EOF

# Note: this does not push to github, it pushes to the local repo
echo 'Listing remotes:'
git remote -v
echo 'About to push these changes to the "origin" remote listed above.'
echo -n 'Enter y to push to "origin".  '
read answer
if [ "y" != "$answer" ] ; then
    echo 'Did not enter y.  Exiting without pushing.'
    exit 1
fi

git push origin "$PAGE_BRANCH"
