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
declare -r HTDOC_DIR=$MAVEN{htmlchunk.output.dir}
declare -r JAVADOC_DIR=$MAVEN{project.build.directory}/javadocs
declare -r CLONE_DIR=/tmp/titanpages

if [ -z "${SCM_URL:-}" -o -z "${SCM_TAG:-}" ]; then
    if [ -z "${1:-}" ]; then
        echo "Usage: $0 path/to/release.properties"
        exit -1
    else
        # Change directory to the folder containing release.properties,
        # store the full path to that directory, then clone it to $CLONE_DIR.
        pushd "`dirname $1`" >/dev/null
        declare -r SCM_URL="`pwd`"
        popd >/dev/null
        echo Read SCM_URL from $1: $SCM_URL

        # Search release.properties for the current release tag.
        declare -r SCM_TAG=`sed -rn 's|\\\\||g; s|^scm\.tag=||p' $1`
        echo Read SCM_TAG from $1: $SCM_TAG
    fi
else
    echo Read SCM_URL from environment: $SCM_URL
    echo Read SCM_TAG from environment: $SCM_TAG
fi

# Create gh-pages branch in the source repo if it doesn't exist.
if [ ! -e .git/refs/heads/"$PAGE_BRANCH" ]; then
    git branch "$PAGE_BRANCH" origin/"$PAGE_BRANCH"
fi

# Clone repo, change into its directory, checkout gh-pages branch
echo Cloning $SCM_URL
git clone "$SCM_URL" "$CLONE_DIR"
cd "$CLONE_DIR"
echo Checking out $PAGE_BRANCH
git fetch origin refs/remotes/origin/"$PAGE_BRANCH":refs/heads/"$PAGE_BRANCH"
git checkout "$PAGE_BRANCH"
git pull origin "$PAGE_BRANCH"

# Wikidoc is an anachronism (we've moved to AsciiDoc), but URLs are
# forever...
echo Copying chunked htmldocs to wikidoc/$SCM_TAG
cd "$CLONE_DIR"/wikidoc
cp -a "$HTDOC_DIR" "$SCM_TAG"
echo Making wikidoc/current a copy of wikidoc/$SCM_TAG
git rm -qr current
cp -a   "$SCM_TAG" current
echo Adding wikidoc changes to git index
git add "$SCM_TAG" current

echo Copying javadoc to javadoc/$SCM_TAG
cd "$CLONE_DIR"/javadoc
cp -a "$JAVADOC_DIR" "$SCM_TAG"
echo Making javadoc/current a copy of javadoc/$SCM_TAG
git rm -qr current
cp -a   "$SCM_TAG" current
echo Adding javadoc changes to git index
git add "$SCM_TAG" current

echo Copying new gh-pages index.html to root
cd $CLONE_DIR
cp "$MAIN_INDEX" index.html
echo Adding new gh-pages index.html file to the git index
git add index.html

git commit -m "Page updates for $SCM_TAG"
git push origin "$PAGE_BRANCH"

cat <<EOF
Locally committed new javadoc, wikidoc, and index.html reflecting release
version $SCM_TAG into gh-pages.

  Clone directory = $CLONE_DIR
  Parent repo     = $SCM_URL
  Branch modified = $PAGE_BRANCH
  Release version = $SCM_TAG

These changes have been pushed to the parent repository.
EOF
