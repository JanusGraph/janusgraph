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
    # http://fedoraproject.org/wiki/Packaging:NamingGuidelines#Package_Versioning
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
    # http://www.debian.org/doc/debian-policy/ch-controlfields.html#s-f-Version
    DEB_RELEASE='~snap'"`date +%Y%m%d`"
else
    # Copy release number
    DEB_RELEASE="$VER_PKG_RELEASE"
fi

export DEB_VERSION DEB_RELEASE
