#!/bin/bash
#
# Copyright 2023 JanusGraph Authors
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

set -e

if [ "$#" -lt "3" ]; then
  echo "usage: build-and-push-image.sh version base-image build-path push/no-push tag-suffix"
  exit 1
fi

JANUS_VERSION=$1
BASE_IMAGE=$2
BUILD_PATH=$3
TAG_SUFFIX=${5:-}
PUSH="${4:-no-push}"

REVISION="$(git rev-parse --short HEAD)"
CREATED="$(date -u +”%Y-%m-%dT%H:%M:%SZ”)"
DOCKERHUB_IMAGE_NAME="docker.io/janusgraph/janusgraph"
GHCR_IMAGE_NAME="ghcr.io/janusgraph/janusgraph"
PLATFORMS="linux/amd64,linux/arm64"

echo "REVISION: ${REVISION}"
echo "CREATED: ${CREATED}"
echo "DOCKERHUB_IMAGE_NAME: ${DOCKERHUB_IMAGE_NAME}"
echo "GHCR_IMAGE_NAME: ${GHCR_IMAGE_NAME}"
echo "JANUS_VERSION: ${JANUS_VERSION}"
echo "BASE_IMAGE: ${BASE_IMAGE}"
echo "BUILD_PATH: ${BUILD_PATH}"
echo "TAG_SUFFIX: ${TAG_SUFFIX}"

# enable buildkit
export DOCKER_BUILDKIT=1
TAGS=()
BUILD_ARGS=()
BUILD_ARGS+=("JANUS_VERSION=$JANUS_VERSION")
BUILD_ARGS+=("REVISION=$REVISION")
BUILD_ARGS+=("CREATED=$CREATED")
BUILD_ARGS+=("BUILD_PATH=$BUILD_PATH")
BUILD_ARGS+=("BASE_IMAGE=$BASE_IMAGE")
ARGS=""
if [[ $PUSH == "push" ]]; then
    ARGS="${ARGS} --push"
fi
if [[ $MULTI_PLATFORM == "true" ]]; then
    ARGS="${ARGS} --platform ${PLATFORMS}"
fi
if [[ $JANUS_VERSION == *"${REVISION}"* ]]; then
    TAGS+=("${JANUS_VERSION}${TAG_SUFFIX}")
else
    TAGS+=("${JANUS_VERSION}-${REVISION}${TAG_SUFFIX}")
    if [[ $PUSH == "no-push" ]]; then
        TAGS+=("${JANUS_VERSION}${TAG_SUFFIX}")
    fi
fi
for BUILD_ARG in "${BUILD_ARGS[@]}"
do
    ARGS="${ARGS} --build-arg ${BUILD_ARG}"
done
for TAG in "${TAGS[@]}"
do
    ARGS="${ARGS} -t ${DOCKERHUB_IMAGE_NAME}:${TAG} -t ${GHCR_IMAGE_NAME}:${TAG}"
done

# build and push the multi-arch image
# unfortunately, when building a multi-arch image, we have to push it right after building it,
# rather than save locally and then push it. see https://github.com/docker/buildx/issues/166
docker buildx build -f "docker/Dockerfile" ${ARGS} .
