#!/bin/bash -eu
#
# Copyright 2017 JanusGraph Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

##############################################################################
#
# Usage (defaults to https://github.com/JanusGraph/janusgraph project):
#
#     $ cd janusgraph
#     $ env COVERITY_SCAN_TOKEN="..." COVERITY_EMAIL="..." \
#           analysis/coverity-scan.sh
#
# This script assumes that:
#
# * you are scanning a GitHub project
# * your project is a Java project using Maven
# * you have a `pom.xml` file at the root of your repo
# * you are running the build on a Linux 64-bit machine
#
# To use for another GitHub repository, run as follows:
#
#     $ cd my-repo
#     $ env COVERITY_SCAN_TOKEN="..." COVERITY_EMAIL="..." \
#           GITHUB_ORG="..." GITHUB_PROJECT="..." \
#           analysis/coverity-scan.sh
#
# Note: your Coverity project is assumed to be of the form:
#
#     "${GITHUB_ORG}/${GITHUB_PROJECT}"
#
# If that is not the case, you can override it as well on the command line via
# the COVERITY_PROJECT env var.
#
# All other settings are either constants in this script or derived from the
# repository, the `pom.xml` file, or other sources.
#
##############################################################################

declare -r COVERITY_TOOL_TAR_GZ="coverity_tool.tar.gz"
declare -r COVERITY_ANALYSIS_DIR="cov-analysis-linux64"

# Required to be `cov-int` by Coverity docs:
# https://scan.coverity.com/download
declare -r COVERITY_BUILD_OUTPUT_DIR="cov-int"
declare -r COVERITY_TAR_GZ="cov-int.tar.gz"

if [ -z "${COVERITY_SCAN_TOKEN:-}" ]; then
  echo "Error: env var COVERITY_SCAN_TOKEN not specified; exiting." >&2
  exit 1
elif [ -z "${COVERITY_EMAIL:-}" ]; then
  echo "Error: env var COVERITY_EMAIL not specified; exiting." >&2
  exit 2
fi

# Get the version number from the `pom.xml` file rather than hardcoding it here; see
# https://stackoverflow.com/questions/41114695/get-pom-xml-version-with-xmllint
# for more info. Note that simpler, traditional XPath-based solutions such as
# https://stackoverflow.com/questions/15461737/how-to-execute-xpath-one-liners-from-shell
# don't work, because Maven's `pom.xml` files use namespaces.
declare -r PROJECT_VERSION="$(xmllint --xpath '/*[local-name()="project"]/*[local-name()="version"]/text()' pom.xml)"
declare -r COVERITY_VERSION="${PROJECT_VERSION}/$(date +'%Y-%m-%dT%H:%M:%S')"

declare -r GIT_HASH="$(git rev-parse HEAD)"
declare -r COVERITY_DESCRIPTION="Automatic build upload via script; git hash: ${GIT_HASH}"

declare -r GITHUB_ORG="${GITHUB_ORG:-JanusGraph}"
declare -r GITHUB_PROJECT="${GITHUB_PROJECT:-janusgraph}"
declare -r COVERITY_PROJECT="${GITHUB_ORG}%2F${GITHUB_PROJECT}"

if ! [ -f "${COVERITY_TOOL_TAR_GZ}" ]; then
  echo "Downloading Coverity analysis tool ..."
  curl \
    -X POST --data "token=${COVERITY_SCAN_TOKEN}&project=${COVERITY_PROJECT}" \
    -o "${COVERITY_TOOL_TAR_GZ}" -s \
    https://scan.coverity.com/download/linux64
else
  echo "Coverity tool tarball already exists; skipping download."
fi

if ! [ -d "${COVERITY_ANALYSIS_DIR}" ]; then
  mkdir "${COVERITY_ANALYSIS_DIR}"
  echo "Uncompressing archive ..."
  # Sample file contents are of the form: "./cov-analysis-linux64-2017.07/bin/..."
  tar zx --strip-components=2 --directory="${COVERITY_ANALYSIS_DIR}" -f "${COVERITY_TOOL_TAR_GZ}"
else
  echo "Coverity tool dir already exists; skipping uncompress step."
fi

if ! [ -d "${COVERITY_BUILD_OUTPUT_DIR}" ]; then
  echo "Running Maven build with Coverity analysis ..."
  # reconfigure Coverity to skip files that cause build errors
  "${COVERITY_ANALYSIS_DIR}"/bin/cov-configure --delete-compiler-config template-javac-config-0
  "${COVERITY_ANALYSIS_DIR}"/bin/cov-configure --delete-compiler-config template-java-config-0
  "${COVERITY_ANALYSIS_DIR}"/bin/cov-configure --java \
    --xml-option=skip_file:AbstractVertex.java \
    --xml-option=skip_file:CacheVertex.java \
    --xml-option=skip_file:CacheVertexProperty.java \
    --xml-option=skip_file:EdgeLabelVertex.java \
    --xml-option=skip_file:GraphDatabaseConfiguration.java \
    --xml-option=skip_file:ImplicitKey.java \
    --xml-option=skip_file:JanusGraph.java \
    --xml-option=skip_file:JanusGraphSchemaVertex.java \
    --xml-option=skip_file:JanusGraphVertex.java \
    --xml-option=skip_file:PreloadedVertex.java \
    --xml-option=skip_file:PropertyKeyVertex.java \
    --xml-option=skip_file:RelationTypeVertex.java \
    --xml-option=skip_file:StandardJanusGraphTx.java \
    --xml-option=skip_file:StandardVertex.java \
    --xml-option=skip_file:StaticArrayBuffer.java \
    --xml-option=skip_file:VertexLabelVertex.java
  # run Coverity build
  "${COVERITY_ANALYSIS_DIR}"/bin/cov-build \
    --dir "${COVERITY_BUILD_OUTPUT_DIR}" \
    --java-cmd-line-buf-size 102400 \
    mvn -DskipTests=true -Dmaven.test.skip=true -Dmaven.javadoc.skip=true -B -V clean install
else
  echo "Maven build already done; remove the '${COVERITY_BUILD_OUTPUT_DIR}' directory to re-run."
fi

echo "Creating archive for uploading to Coverity ..."
tar czf "${COVERITY_TAR_GZ}" "${COVERITY_BUILD_OUTPUT_DIR}"

echo "Uploading results to Coverity ..."
curl \
  --form token="${COVERITY_SCAN_TOKEN}" \
  --form email="${COVERITY_EMAIL}" \
  --form file="@${COVERITY_TAR_GZ}" \
  --form version="${COVERITY_VERSION}" \
  --form description="${COVERITY_DESCRIPTION}" \
  -s \
  "https://scan.coverity.com/builds?project=${COVERITY_PROJECT}"

echo "Done."
