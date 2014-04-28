#!/bin/bash

#
# Update gh-pages during a release.
#
# Usage: gh-pages-update.sh release.properties
#
# Clones the repo specified by scm.url in release.properties.
# 
# Checks out the gh-pages branch.
#
# Copies javadoc and wikidoc into (java|wiki)doc/scm.tag from
# release.properties}/.
#
# Updates (java|wiki)doc/current/ to be symlinks to new docs.
#
# Copies target/site-resources/index.html to base of gh-pages.
#
# Does not commit or push anything.
#

[ -z "$1" ] && { echo "Usage: $0 path/to/release.properties"; exit -1; }

set -e
set -u

echo Reading settings from $1
# Use this to clone the github repo (already pushed release)
#declare -r SCM_URL=`sed -rn 's|\\\\||g; s|^scm\.url=scm:git:||p' $1`
# Or this to clone the local repo (haven't pushed release yet)
declare -r SCM_URL="file://${project.basedir}"
declare -r SCM_TAG=`sed -rn 's|\\\\||g; s|^scm\.tag=||p' $1`
echo Read SCM_URL: $SCM_URL
echo Read SCM_TAG: $SCM_TAG

declare -r PAGE_BRANCH=gh-pages
declare -r CLONE_DIR=${project.build.directory}/pages
declare -r JAVADOC_DIR=${project.build.directory}/apidocs
declare -r WIKIDOC_DIR=${project.basedir}/doc/html
declare -r WIKI_INDEX=${project.build.directory}/release-resources/wikidoc-index.html
declare -r MAIN_INDEX=${project.build.directory}/release-resources/index.html

echo Cloning $SCM_URL
git clone "$SCM_URL" "$CLONE_DIR"
cd "$CLONE_DIR"
echo Checking out $PAGE_BRANCH
git checkout "$PAGE_BRANCH"

echo Copying generated wikidoc to wikidoc/$SCM_TAG
cd "$CLONE_DIR/wikidoc"
cp -a "$WIKIDOC_DIR" "$SCM_TAG"
cp "$WIKI_INDEX" "$SCM_TAG"/index.html
echo Deleting and recreating wikidoc/current for $SCM_TAG
git rm -r current
cp -a "$SCM_TAG" current
echo Adding wikidoc changes to git index
git add "$SCM_TAG" current

echo Copying generated javadoc to javadoc/$SCM_TAG
cd "$CLONE_DIR/javadoc"
cp -a "$JAVADOC_DIR" "$SCM_TAG"
echo Deleting and recreating javadoc/current $SCM_TAG
git rm -r current
cp -a "$SCM_TAG" current
echo Adding javadoc changes to git index
git add "$SCM_TAG" current

echo Copying new gh-pages index.html to root
cd "$CLONE_DIR"
cp "$MAIN_INDEX" index.html
echo Adding new gh-pages index.html file to the git index
git add index.html

cat <<EOF

Locally staged new javadoc, wikidoc, and index.html reflecting release
version $SCM_TAG into gh-pages.

  Clone directory = $CLONE_DIR
  Remote repo     = $SCM_URL
  Branch modified = $PAGE_BRANCH
  Release version = $SCM_TAG

All changes have been added to the git index in this clone.  The
working directory should be clean.  No changes have been committed or
pushed.

Check changes with the following commands:

  cd "$CLONE_DIR"
  git status
  git ls-files --cached --stage # shows filemodes
  git diff --cached # voluminous output

If all looks well, commit and push:

  git commit -m "Page updates for $SCM_TAG"
  git push origin $PAGE_BRANCH
EOF



