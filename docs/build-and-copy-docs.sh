#!/bin/bash -u
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

################################################################################

# This script assumes the existence of two checked out Git repos:
# JanusGraph/janusgraph and JanusGraph/docs.janusgraph.org
#
# It will build the docs in the first repo and copy them to the second repo, such
# that a PR can be created in the second repo and pushed for review. Once the
# PR in docs.janusgraph.org is submitted, it is published to
# http://docs.janusgraph.org within minutes by GitHub since that domain is hosted
# by GitHub directly from the repo.

################################################################################

# If your checked out JanusGraph Git repos under the same dir, e.g.,
# ${GITHUB_ROOT}/janusgraph, then you can run this script as follows:
#
#     env GITHUB_ROOT=[...] path/to/this/script.sh
#
# If on top of the above, your ${GITHUB_ROOT} == ${HOME}/github, you can run
# this script simply as-is:
#
#     path/to/this/script.sh
#
# If needed, you can modify the variables below when running this script to
# customize your directories or options (such as documentation version to
# update) as follows:
#
#     env VAR=[...] VAR2=[...] path/to/this/script.sh
#
declare -r GITHUB_ROOT="${GITHUB_ROOT:-${HOME}/github}"
declare -r SOURCE_REPO_DIR="${SOURCE_REPO_DIR:-${GITHUB_ROOT}/janusgraph/janusgraph}"
declare -r VERSION="$(xmllint --xpath '/*[local-name()="project"]/*[local-name()="version"]/text()' "${SOURCE_REPO_DIR}/pom.xml")"
declare -r DOCS_REPO_DIR="${DOCS_REPO_DIR:-${GITHUB_ROOT}/janusgraph/docs.janusgraph.org}"
declare -r DOCS_REPO_VERSION_DIR="${DOCS_REPO_DIR}/${VERSION}"

if ! [ -d "${SOURCE_REPO_DIR}" ]; then
  echo "Source repo dir: ${SOURCE_REPO_DIR} does not exist; exiting." >&2
  exit 1
elif ! [ -d "${DOCS_REPO_VERSION_DIR}" ]; then
  mkdir -p "${DOCS_REPO_VERSION_DIR}"
fi

pushd "${SOURCE_REPO_DIR}" >& /dev/null 2>&1

echo "Running 'mvn clean' ..."
declare -r MVN_CLEAN_LOG="$(mktemp /tmp/janusgraph-doc.mvn-clean.XXXXXX)"
mvn clean -DskipTests=true -pl janusgraph-doc -am > "${MVN_CLEAN_LOG}" 2>&1
if [ $? -ne 0 ]; then
  echo "'mvn clean' failed; see ${MVN_CLEAN_LOG} for more info; exiting" >&2
  exit 2
else
  echo "'mvn clean' step succeeded; continuing."
fi

echo "Running 'mvn install' ..."
declare -r MVN_INSTALL_LOG="$(mktemp /tmp/janusgraph-doc.mvn-install.XXXXXX)"
mvn install -DskipTests=true -pl janusgraph-doc -am > "${MVN_INSTALL_LOG}" 2>&1
if [ $? -ne 0 ]; then
  echo "'mvn install' failed; see ${MVN_INSTALL_LOG} for more info; exiting" >&2
  exit 2
else
  echo "'mvn install' step succeeded; continuing."
fi

echo "Deleting all files in ${DOCS_REPO_VERSION_DIR}/* ..."
rm -rf "${DOCS_REPO_VERSION_DIR}"/*
echo "Copying generated docs to 'docs.janusgraph.org' repo ..."
cp -r janusgraph-doc/target/docs/chunk/* "${DOCS_REPO_VERSION_DIR}"
echo "Done."

popd >& /dev/null 2>&1
