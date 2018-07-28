Releasing JanusGraph
====================

Prerequisites
-------------

The release process has only been tested on Linux.  The following
tools must be installed.

* [expect](http://expect.sourceforge.net/)
* [gpg](http://www.gnupg.org/) with a running agent

~/.m2/settings.xml will need the following entries.

```xml
<settings>
  <servers>
    <server>
      <id>ossrh</id>
      <username>...</username>
      <password>...</password>
    </server>
  </servers>
</settings>
```

Release Steps
-------------

### Preparing Documentation

Update version-sensitive files in the root and documentation sources
in the `docs` subdirectory:

* `docs/changelog.txt`
* `docs/versions.txt`
* `docs/upgrade.txt`

Use the [`docs/build-and-copy-docs.sh`](docs/build-and-copy-docs.sh) script to
build a set of docs for this release and copy them to the cloned
`docs.janusgraph.org` repo which you will update later.

You may also need to update the following files in the main repo for any new
or updated dependencies:

* `NOTICE.txt`

### Preparing the Local Repository

Commit or stash any uncommitted changes.

Recommended but not required:

* Delete untracked files and directories in the clone by running
  <code>git clean -fd</code>

### Deploying Artifacts & Archives

```bash
# This script does several things.
#
# * Prompts for a ${janusgraph.compatible.versions} update
# * Locally commits the release using the release plugin
# * Deploys Maven artifacts to Sonatype OSS (staging, not released yet)
# * Uploads zipfiles to S3
# * Locally commits gh-pages updates (index.html and javadocs)
# * Uploads AsciiDoc-generated documentation to S3
#
# Although it uploads to Sonatype OSS Staging and S3, it does
# not push to github nor does it release the Sonatype repo.
# This step is still essentially reversible: just destroy the
# local commit history, drop the Sonatype OSS Staging repo, and
# delete the files uploaded to S3.
janusgraph-dist/src/release/release.sh
```

### Checking Artifacts & Archives

This is a good time to inspect the archives just uploaded to S3. Directory
listing is normally disabled on that directory, so you may need to login to the
S3 console to list the contents and check that nothing's out of place.

If S3 looks good, then inspect and close the staging repository that
the deploy phase automatically created on https://oss.sonatype.org/.

### Pushing the Release

*This is the point of no return.* After releasing artifacts to Maven
Central and pushing history to the public Github repo, the release
can't be canceled.

Release the Sonatype OSS repository that you inspected and closed
earlier.  It will appear on Maven Central in an hour or two.

To release the JanusGraph.Net.Extensions NuGet package to [nuget.org](https://www.nuget.org)
you need an account on nuget.org and be added as an owner to the package.
Ask a member of the Technical Steering Committee who is already listed as
an owner of that package for that. The publishing of the package also
requires an [API key that can be created](https://docs.microsoft.com/en-us/nuget/create-packages/publish-a-package#create-api-keys)
for your account on nuget.org.
With this API key, you can publish the NuGet package:

```bash
mvn deploy -pl :janusgraph-dotnet-source -Dnuget -Ddotnet.repository.key=[api-key]
```

Finally, push your local changes to Github:

```bash
# cd to the janusgraph repository root if not already there
git push origin $BRANCH_NAME
git push origin refs/tags/$RELEASE_VERSION
git push origin gh-pages
```

Update these pages on the Github wiki:

* Home
* Downloads

### Deploy a New Snapshot

To kickoff the next round of development, deploy a copy of the new
SNAPSHOT version's artifacts to the Sonatype OSS snapshots repository.

```bash
# From the repository root
git checkout $BRANCH_NAME
mvn clean deploy -Pjanusgraph-release
```
