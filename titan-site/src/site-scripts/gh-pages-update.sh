#!/bin/bash

#
# Update gh-pages during a release.
#
# Usage: gh-pages-update.sh release.properties
#
# Clones the repo whose root is the directory containing
# release.properties.  Checks out the gh-pages branch in
# the clone.
#
# Copies javadoc and wikidoc into (java|wiki)doc/scm.tag from
# release.properties}/.
#
# Deletes (java|wiki)doc/current/ and replaces them with copies of the
# (java|wiki)doc/scm.tag/ folder from above.  Earlier versions of this
# script tried using symlinks instead of directory copies, but while
# git can handle symlinks, and while github's file browser can handle
# symlinks, gh-pages cannot and 404s on them.
#
# Copies target/site-resources/index.html to base of gh-pages.
#
# Does not commit or push anything.
#

set -e
set -u

# Set some path constants
declare -r PAGE_BRANCH=gh-pages
declare -r WIKI_INDEX=$MAVEN{project.build.directory}/site-resources/wikidoc-index.html
declare -r MAIN_INDEX=$MAVEN{project.build.directory}/site-resources/index.html
declare -r CLONE_DIR=/tmp/titanpages

if [ -z "${1:-}" ]; then
    echo "Usage: $0 path/to/release.properties"
    exit -1
fi

# Change directory to the folder containing release.properties,
# store the full path to that directory, then clone it to $CLONE_DIR.
pushd "`dirname $1`" >/dev/null
declare -r SCM_URL="`pwd`"
popd >/dev/null
echo Cloning $SCM_URL
git clone "$SCM_URL" "$CLONE_DIR"

# If the gh-pages branch doesn't already exist, then create it.
# The branch must exist for the sake of the clone we're about to make.
if [ ! -e .git/refs/heads/"$PAGE_BRANCH" ]; then
    git branch "$PAGE_BRANCH" origin/"$PAGE_BRANCH"
fi

# Search release.properties for the current release tag.
declare -r SCM_TAG=`sed -rn 's|\\\\||g; s|^scm\.tag=||p' $1`
echo Set SCM_TAG from $1: $SCM_TAG
declare -r HTDOC_ZIP=$MAVEN{project.build.directory}/titan-site-"$SCM_TAG"-htdocs.zip

# Change directory to the clone and check out gh-pages.
cd "$CLONE_DIR"
echo Checking out $PAGE_BRANCH
git checkout "$PAGE_BRANCH"

echo Unzipping wikidoc to wikidoc/$SCM_TAG
cd "$CLONE_DIR/wikidoc"
mkdir "$SCM_TAG"
cd "$CLONE_DIR/wikidoc/$SCM_TAG"
unzip -q "$HTDOC_ZIP"
rm -rf javadoc # included in -htdocs.zip but not needed here
cp "$WIKI_INDEX" index.html
cd $CLONE_DIR/wikidoc
echo Making wikidoc/current a copy of wikidoc/$SCM_TAG
git rm -qr current
cp -a   "$SCM_TAG" current
echo Adding wikidoc changes to git index
git add "$SCM_TAG" current

echo Unzipping javadoc to javadoc/$SCM_TAG
cd $CLONE_DIR/javadoc
mkdir "$SCM_TAG"
cd $CLONE_DIR/javadoc/"$SCM_TAG"
unzip -q "$HTDOC_ZIP"
# Move javadoc to a temp dir one directory level higher
mv javadoc $CLONE_DIR/javadoc/javadoc.tmp
cd $CLONE_DIR/javadoc
# Delete the wikidoc stuff
rm -rf "$SCM_TAG"
# Move temp dir into place as new $SCM_TAG dir
mv javadoc.tmp "$SCM_TAG"
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
