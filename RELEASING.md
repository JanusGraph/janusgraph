Releasing Titan
===============

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
    <server>
      <id>aurelius.s3</id>
      <username>AWS Key ID</username>
      <password>AWS Secret Key</password>
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
* titan-site/src/site-resources/index.html
  (this template generates a new http://thinkaurelius.github.io/titan/)

Some of these updates could potentially be automated, but tweaking
documentation before a release will always require at least human in
the loop.

### Preparing the Local Repository

Commit or stash any uncommitted changes.

Recommended but not required:

* Push any unpushed commits on master with <code>git push origin
  master</code>
* Delete untracked files and directories in the clone by running
  <code>git clean -fd</code>

### Deploying Artifacts & Archives

```bash
# This step creates local commits and a local tag.  It also
# uploads archives to S3 and artifacts to Sonatype Staging.
# It does not push to github, nor does it release artifacts
# to Maven Central.  Only the archives in S3 are publicly
# visible, and even then only by those who have the exact URL
# of the archives (the bucket is not publicly listable).
# In that sense, this step is reversible: the local commits
# and tag and the staged artifacts and archives can be safely
# deleted if it does not turn out as expected.
titan-dist/src/release/release.sh
```

### Checking Artifacts & Archives

This is a good time to inspect the archives just uploaded to
http://s3.thinkaurelius.com/downloads/titan/.  Directory listing is
normally disabled on that directory, so you may need to login to the
S3 console to list the contents and check that nothing's out of place.

If S3 looks good, then inspect and close the staging repository that
the deploy phase automatically created on https://oss.sonatype.org/.

If you decide to abort at this stage, then:

* Delete any artifacts just uploaded to S3
* Drop the staging repository in Sonatype OSS
* Run the following commands to delete your local git changes

```bash
#
#                        WARNING
#
# This will obliterate all local commits and uncommitted files.
# Only copy and paste this code segment if you want to abort a
# partially-completed release.  Replace $RELEASE_VERSION by the
# version just attempted.
git checkout master
git reset --hard origin/master
git tag -d $RELEASE_VERSION
git clean -fd
```

Assuming everything looked fine on OSS Sonatype and S3, we'll move on
to updating gh-pages locally.

### Generating new Javadoc & Wikidoc for gh-pages

```bash
# The following steps through
# `git diff origin/gh-pages..gh-pages` were current as of 0.4.x
# but are out-of-date as of 0.5.0.  The 0.5.0 release made some
# changes to the gh-pages contents outside the template and
# script described below, and these changes haven't been copied
# into the template and script, so running these steps could squash
# the changes.  The steps are retained for future reference, but
# these commands should not be run as-is in 0.5.x.

# This clones the repo to /tmp/titanpages.  It checks out the gh-pages
# branch, then populates wikidoc/$RELEASE_VERSION/ and
# javadoc/$RELEASE_VERSION/ with a copy of the HTML produced from
# AsciiDoc sources and a copy of the current javadoc (respectively).
# It also replaces the wikidoc/current/ and javadoc/current/
# directories with copies of files for $RELEASE_VERSION.  "wikidoc" is
# an anachronism, since the docs are now AsciiDoc based, but it's
# retained for URL uniformity with previous releases of the docs.
# Finally, it generates a new index.html.  It commits these changes
# and pushes them from /tmp/titanpages to the original local repo.  It
# does not push anything to github.

# Why clone to /tmp/titanpages?  We need to modify the gh-pages
# branch, but we don't want to mess with the repository that's
# currently in the middle of releasing Titan, and which might have
# uncommitted changes that cause problems if we try to checkout the
# gh-pages branch.  Cloning the repo to /tmp and then checking out
# gh-pages in the clone guarantees a clean working copy and thereby
# precludes these conflicts.

#titan-dist/target/release/gh-pages-update.sh release.properties
# If you want to check this script's work, then consider running
# the following:
#git diff --name-status origin/gh-pages..gh-pages
#git diff origin/gh-pages..gh-pages | less # lots of text
```

### Release to Maven Central and Push to Github

Release the Sonatype OSS repository that you inspected and closed
earlier.  It will appear on Maven Central in an hour or two.

Finally, push your local changes to Github:

```bash 
# cd to the titan repository root if not already there
git push origin master
git push origin refs/tags/$RELEASE_VERSION
#git push origin gh-pages
```

Update these pages on the Github wiki:

* Home
* Downloads

### Deploy a New Snapshot

To kickoff the next round of development, deploy a copy of the new
SNAPSHOT version's artifacts to the Sonatype OSS snapshots repository.

```bash
# From the repository root
git checkout master
mvn clean deploy
```
