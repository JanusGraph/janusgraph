#!/bin/bash

set -u

echo "Reading current HEAD..."
OLD_HEAD="`git symbolic-ref --short HEAD 2>/dev/null`"
if [ $? -eq 0 ] ; then
    echo "Stored HEAD $OLD_HEAD."
else
    OLD_HEAD=''
    echo 'Could not resolve symbolic ref for HEAD -- detached?'
fi

set -e # No errors allowed from here to end

echo Reading settings from $1
declare -r SCM_TAG=`sed -rn 's|\\\\||g; s|^scm\.tag=||p' $1`
declare -r TAG_REF=refs/tags/"$SCM_TAG"
declare -r NEW_ANNOTATION="JanusGraph $SCM_TAG"
echo Read SCM_TAG: "$SCM_TAG"

echo Listing $TAG_REF '(before)'...
git tag -ln "$SCM_TAG"

echo Checking out $TAG_REF...
git checkout "$TAG_REF"
echo Deleting tag $SCM_TAG...
git tag -d "$SCM_TAG"
echo "Creating unsigned tag \"$SCM_TAG\" with annotation \"$NEW_ANNOTATION\"..."
git tag "$SCM_TAG" -m "$NEW_ANNOTATION"

if [ -n "$OLD_HEAD" ]; then
    echo "Moving HEAD back to $OLD_HEAD..."
    git checkout "$OLD_HEAD"
fi

echo Listing $TAG_REF '(after)'...
git tag -ln "$SCM_TAG"
