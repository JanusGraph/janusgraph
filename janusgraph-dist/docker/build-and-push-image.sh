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

if [ "$#" -gt "4" ]; then
  echo "usage: build-and-push-image.sh version base-image build-path tag-suffix [push]"
  exit 1
fi

JANUS_VERSION=$1
BASE_IMAGE=$2
BUILD_PATH=$3
TAG_SUFFIX=$4
PUSH="${5:-false}"

REVISION="$(git rev-parse --short HEAD)"
CREATED="$(date -u +”%Y-%m-%dT%H:%M:%SZ”)"
IMAGE_NAME="docker.io/janusgraph/janusgraph"
PLATFORMS="linux/amd64,linux/arm64"

echo "REVISION: ${REVISION}"
echo "CREATED: ${CREATED}"
echo "IMAGE_NAME: ${IMAGE_NAME}"
echo "JANUS_VERSION: ${JANUS_VERSION}"
echo "BASE_IMAGE: ${BASE_IMAGE}"
echo "BUILD_PATH: ${BUILD_PATH}"
echo "TAG_SUFFIX: ${TAG_SUFFIX}"

# enable buildkit
export DOCKER_BUILDKIT=1

ARGS=""
if [[ $PUSH == "push" ]]; then
    ARGS="${ARGS} --push"
fi
if [[ $MULTI_PLATFORM == "true" ]]; then
    ARGS="${ARGS} --platform ${PLATFORMS}"
fi
if [[ $LATEST == "true" ]] && [[ $TAG_SUFFIX == "" ]]; then
    ARGS="${ARGS} -t ${IMAGE_NAME}:latest"
fi
ARGS="${ARGS} -t ${IMAGE_NAME}:${JANUS_VERSION}${TAG_SUFFIX} -t ${IMAGE_NAME}:${JANUS_VERSION}${TAG_SUFFIX}-${REVISION}"

# build and push the multi-arch image
# unfortunately, when building a multi-arch image, we have to push it right after building it,
# rather than save locally and then push it. see https://github.com/docker/buildx/issues/166
docker buildx build -f "docker/Dockerfile" --build-arg JANUS_VERSION="$JANUS_VERSION" --build-arg REVISION="$REVISION" --build-arg CREATED="$CREATED" --build-arg BUILD_PATH="$BUILD_PATH" --build-arg BASE_IMAGE="$BASE_IMAGE" ${ARGS} .
