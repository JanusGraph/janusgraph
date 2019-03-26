# Copyright 2019 JanusGraph Authors
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
# limitations under the License

declare -r VER=${project.version}
declare -r VER_MAJOR=${parsedVersion.majorVersion}
declare -r VER_MINOR=${parsedVersion.minorVersion}
declare -r VER_PATCH=${parsedVersion.incrementalVersion}
declare -r VER_QUALIFIER=${parsedVersion.qualifier}
declare -r VER_BUILDNUMBER=${parsedVersion.buildNumber}
declare -r VER_PKG_RELEASE=${pkg.release}

# Set .rpm package version
RPM_VERSION="$VER_MAJOR.$VER_MINOR.$VER_PATCH"

# Set .rpm release value
if [ "SNAPSHOT" = "$VER_QUALIFIER" ]; then
    # Generate Fedora-compliant pre-release string from pkgcommon/etc/version.sh
    # https://fedoraproject.org/wiki/Packaging:NamingGuidelines#Package_Versioning
    RPM_RELEASE="0.1.`date +%Y%m%d`snap"
else
    # Copy release number
    RPM_RELEASE="$VER_PKG_RELEASE"
fi

export RPM_VERSION RPM_RELEASE

# Set .deb package version
DEB_VERSION="$VER_MAJOR.$VER_MINOR.$VER_PATCH"

# Set .deb package release
if [ "SNAPSHOT" = "${VER_QUALIFIER}" ]; then
    # Generate Debian-policy-compliant pre-release string
    # https://www.debian.org/doc/debian-policy/ch-controlfields.html#s-f-Version
    DEB_RELEASE='~snap'"`date +%Y%m%d`"
else
    # Copy release number
    DEB_RELEASE="$VER_PKG_RELEASE"
fi

export DEB_VERSION DEB_RELEASE
