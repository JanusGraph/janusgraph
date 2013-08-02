#!/bin/bash
cd "$MAVEN{project.parent.basedir}"
tag="`sed -nr 's/scm\.tag=//p' release.properties`"
git checkout refs/tags/"$tag"
git tag -d "$tag"
git tag "$tag" -m "Titan ${tag}"
