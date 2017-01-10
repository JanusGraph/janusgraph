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
      <id>sonatype-nexus-snapshots</id>
      <username>...</username>
      <password>...</password>
    </server>
    <server>
      <id>sonatype-nexus-staging</id>
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
in the docs/ subdirectory off the root.

Documentation pages:

* changelog.txt
* versions.txt
* upgrade.txt
* acknowledgements.txt

Files in the main repo:

* CHANGELOG.asc
* NOTICE.txt
* UPGRADE.asc
* janusgraph-site/src/site-resources/index.html
  (this template generates a new root index page)

Some of these updates could potentially be automated, but tweaking
documentation before a release will always require at least human in
the loop.

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
mvn clean deploy
```
