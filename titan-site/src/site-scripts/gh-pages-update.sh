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

[ -z "$1" ] && { echo "Usage: $0 path/to/release.properties"; exit -1; }

set -e
set -u

echo Reading settings from $1
declare -r SCM_URL=`sed -rn 's|\\\\||g; s|^scm\.url=scm:git:||p' $1`
declare -r SCM_TAG=`sed -rn 's|\\\\||g; s|^scm\.tag=||p' $1`
echo Read SCM_URL: $SCM_URL
echo Read SCM_TAG: $SCM_TAG

declare -r PAGE_BRANCH=gh-pages
declare -r CLONE_DIR=$MAVEN{project.build.directory}/pages
declare -r HTDOC_ZIP=$MAVEN{project.build.directory}/titan-site-"$SCM_TAG"-htdocs.zip
#declare -r JAVADOC_DIR=$MAVEN{project.build.directory}/apidocs
#declare -r WIKIDOC_DIR=$MAVEN{project.basedir}/doc/html
declare -r WIKI_INDEX=$MAVEN{project.build.directory}/site-resources/wikidoc-index.html
declare -r MAIN_INDEX=$MAVEN{project.build.directory}/site-resources/index.html

echo Cloning $SCM_URL
git clone "$SCM_URL" "$CLONE_DIR"
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
