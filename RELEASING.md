# Releasing JanusGraph

## Prerequisites

The release process has only been tested on Linux and macOS.
The following tools must be installed.

*   [gpg](https://www.gnupg.org/) - Requires a running agent.

### Sonatype account

The release artifacts will be deployed into Sonatype OSS with the Maven deploy plugin.
You must have an account with Sonatype, and it must be associated with the JanusGraph org.
See the relevant issue [here.](https://issues.sonatype.org/browse/OSSRH-28274)

#### GPG Signing Key

The JanusGraph artifacts are signed with a GPG signature.  
If you don't already have a GPG signing key included with the `KEYS` file, you will need to create one and update the `KEYS` file.  
If you already have your key included into `KEYS` file then skip this step.  
Go to [releases](https://github.com/JanusGraph/janusgraph/releases) and download the `KEYS` file (which is located in Assets) from the latest release.  
Generate your key:
```Shell
gpg --full-generate-key
```
Select:
*   RSA and RSA
*   4096 bits
*   no expiration
*   comment: `CODE SIGNING KEY`

Add your key to the KEYS file:
```Shell
(gpg --list-sigs <key id> && gpg --armor --export <key id>) >> KEYS.
```

Once your key has been created and added to the KEYS file it needs to be published to a [public key server](https://sks-keyservers.net/status/).  
You can upload to multiple key servers within the pool or use an alternate if `pgp.mit.edu` is down.  
Sonatype typically uses the following key servers: `pool.sks-keyservers.net`, `keyserver.ubuntu.com`, `keys.gnupg.net`.
```Shell
gpg --keyserver <key server> --send-keys <key id>
```
Example: 
```Shell
gpg --keyserver pgp.mit.edu --send-keys 7F87F9BD4D9F71A6
```

### Configure Maven for server passwords

The release artifacts will be uploaded into a staging directory on Sonatype OSS.
If you do not configure Maven with your server passwords, the Maven deploy plugin will run into a 401 Unauthorized error.
Steps below were taken from [this guide.](https://maven.apache.org/guides/mini/guide-encryption.html)

*   Create a master password: `mvn --encrypt-master-password`
*   Add the master password to `$HOME/.m2/settings-security.xml`

```xml
<settingsSecurity>
  <master>{master password}</master>
</settingsSecurity>
```

**Notice:** The password and the encapsulating braces `{}` should be placed into the `<master>` tag. Example: `<master>{1234567ABCDEFG}</master>`

*   Once the master password has been added to `$HOME/.m2/settings-security.xml` ,encrypt a server password: `mvn --encrypt-password`
*   Create `$HOME/.m2/settings.xml` using your Sonatype username and encrypted server password

```xml
<settings>
  <servers>
    <server>
      <id>ossrh</id>
      <username>{sonatype username}</username>
      <password>{encrypted sonatype password}</password>
    </server>
  </servers>
</settings>
```

### GPG passphrase

You can pass it as a command line parameter to Maven with `-Dgpg.passphrase=$GPG_PASS`.
You can also encrypt the gpg key with `mvn --encrypt-password` and add it to `$HOME/.m2/settings.xml` as shown below.
You may still have to enter your GPG passphrase once.
Otherwise you will have to type in your gpg passphrase many times when prompted during the build.

```xml
<settings>
   <servers>
    <server>
      <id>ossrh</id>
        <username>{sonatype username}</username>
        <password>{encrypted sonatype password}</password>
    </server>
  </servers>
  <profiles>
    <profile>
      <id>gpg</id>
      <properties>
        <gpg.executable>gpg</gpg.executable>
        <gpg.passphrase>{encrypted gpg passphrase}</gpg.passphrase>
      </properties>
    </profile>
  </profiles>
  <activeProfiles>
    <activeProfile>gpg</activeProfile>
  </activeProfiles>
</settings>
```

# Release Checklist

*   [ ] Start up new `[DISCUSS]` thread on janusgraph-dev with suggestions on what should be included in the release and target date
*   [ ] Make sure all PRs and issues added since last release are associated to the milestone
*   [ ] Complete all items associated with milestone or move outstanding items to a new milestone if necessary
*   [ ] Create Pull Requests with release tag and version updates
*   [ ] validate all changes have been merged upstream
*   [ ] Generate updated docs and create pull request
*   [ ] Run Deploy to create [Sonatype](https://oss.sonatype.org/#welcome) staging repository and Generate artifacts for release page
*   [ ] Upload artifacts to a draft release
*   [ ] Close the staging repository in [Sonatype](https://oss.sonatype.org/#stagingRepositories)
*   [ ] Convert release draft to a public pre-release
*   [ ] Create `[VOTE]` [janusgraph-dev](https://lists.lfaidata.foundation/g/janusgraph-dev) vote thread and [get required votes of 3 TSC members](https://www.apache.org/foundation/voting.html)
*   [ ] Release the staging repository in Sonatype
*   [ ] Remove pre-release label on the [releases page](https://github.com/JanusGraph/janusgraph/releases/)
*   [ ] Announce the new release
*   [ ] Prepare the next SNAPSHOT release
*   [ ] Document lessons learned

# Release Steps


### Create discussion thread

It is recommended to use previous releases as a template.
Here is a [link](https://groups.google.com/d/msg/janusgraph-dev/BGaCCH1cTCE/gR8OGm32AQAJ) to the discussion thread for the 0.2.1 release.
It's important to highlight what outstanding work items should make it into the release and to establish a release manager.

### Clean up release tags

Go through all the commits that were added since the previous release and make sure they are correctly tagged with the current release.
Any issues closed as a result of those commits should be tagged as part of the release as well.

### Finalize all outstanding work items

Before submitting a pull request with updated version numbers ensure that everything in the [release milestone](https://github.com/JanusGraph/janusgraph/milestones) is closed out.
Any outstanding pull requests that will need to be re-targeted at future milestone.

### Create Pull Requests with release tag and version updates

Before any artifacts can be generated and vote can be made the version number will need to be updated in the pom.xml files.

#### Update configuration reference documentation

```Shell
mvn --quiet clean install -DskipTests=true -pl janusgraph-doc -am
```

#### Update release version

This command will remove `SNAPSHOT` from all versions in `pom` files.

```Shell
mvn versions:set -DremoveSnapshot=true -DgenerateBackupPoms=false
```

After the version has been set, update the scm tag by opening the main `pom.xml` and changing `<tag>HEAD</tag>` to match the release tag.  
Example: `<tag>v0.3.2</tag>`

#### Update version-sensitive files

Update version-sensitive files in the root and documentation sources in the `docs` subdirectory:

* `docs/changelog.md` - Add / finalize release section
* `mkdocs.yml` - Update `latest_version` and check all others package versions. 

You may also need to update the following files in the main repo for any new or updated dependencies:

*   `NOTICE.txt`

#### Create a release commit and a release tag

Create a release commit:
```Shell
git commit -m "JanusGraph release <version> [cql-tests] [tp-tests]" -s
```

After that create a release tag with the next command:
```Shell
git tag -a <release tag> -m ""
```
Example:
```Shell
git tag -a v0.3.2 -m ""
```

#### Create the Pull Request

Open up pull requests for the version updates.
It is recommended to add `[tp-tests]` to the commit message so the TinkerPop test suite will run.
After the updates are approved and merged, continue on with the release process.

# Build Release Artifacts

* Pull down the latest, merged code from GitHub.
* Stash any uncommitted changes.
* Delete untracked files and directories.

```Shell
git fetch
git pull
git stash save
git clean -fdx
```

* Deploy all jars (including javadoc and sources) and all signatures for jars to a staging repository on Sonatype. Notice, this step includes 2 separate commands. The first command will clean repository before generating artifacts and the second command will deploy those generated artifacts. You should not change anything in the repository or run `clean` between the first and the second commands.
```Shell
mvn clean install -Pjanusgraph-release -DskipTests=true
mvn deploy -Pjanusgraph-release -DskipTests=true
```

* Install MkDocs if not installed. It is needed to build documentation.
1. Install `python3` and `pip3` (newest version of pip) 
    * You can also checkout the installation guide of [material-mkdocs](https://squidfunk.github.io/mkdocs-material/getting-started/)
2. Install requirements using `pip3 install -r requirements.txt`

*   Prepare files for GitHub release
```Shell
export JG_VER="janusgraph-0.5.0"
export JG_FULL_VER="janusgraph-full-0.5.0"
mkdir -p ~/jg-staging
cp janusgraph-dist/target/${JG_VER}.zip* ~/jg-staging/
cp janusgraph-dist/target/${JG_FULL_VER}.zip* ~/jg-staging/
mkdocs build
mv site ${JG_VER}-doc
zip -r ${JG_VER}-doc.zip ${JG_VER}-doc
gpg --armor --detach-sign ${JG_VER}-doc.zip
cp ${JG_VER}-doc.zip* ~/jg-staging/
```

If it fails due to Inappropriate ioctl for device error, run:
```Shell
export GPG_TTY=$(tty)
```

*   Verify signature validity (both commands should show good signature)
```Shell
cd ~/jg-staging
gpg --verify ${JG_VER}.zip.asc ${JG_VER}.zip
gpg --verify ${JG_FULL_VER}.zip.asc ${JG_FULL_VER}.zip
gpg --verify ${JG_VER}-doc.zip.asc ${JG_VER}-doc.zip
```

### Create a Draft Release on GitHub

While logged into GitHub go to the [JanusGraph releases page](https://github.com/JanusGraph/janusgraph/releases/) and click the `Draft a new release` button.
Supply the tag number, target branch, and title.
For the description use the previous releases as a template and update the tested versions accordingly.
All of the artifacts that were created and moved to `~/jg-staging/` in the previous step need to be added to the draft release.
Be sure to mark it as `pre-release`.
It is recommended and keep the release in draft until you're ready to start a vote.
In addition to the artifacts in `~/jg-staging/` a `KEYS` file must also be added to the release.

### Close the staging repository

Log into [Sonatype](https://oss.sonatype.org/#welcome) and select Staging Repositories under Build Promotion.
If you recently uploaded you can easily find your staged release by doing a descending sort.
Verify that the contents look complete and then check the release before clicking close.

This step will run verification on all the artifacts.
In particular, this will verify that all of the artifacts have a valid GPG signature.
If this step fails, there are changes that must be made before starting the vote.

### Create a `[VOTE]` Thread
Once all the artifacts have been upload to the GItHub releases page and the staging repository has been populated and closed it's time to create a `[VOTE]` thread.
Here is an example [vote thread from JanusGraph 0.3.1](https://groups.google.com/d/msg/janusgraph-dev/iV8IsUqhcnw/74p3Y7lNAAAJ) that can be used as a template.
Similar to the [Apache release voting policy](https://www.apache.org/foundation/voting.html#ReleaseVotes) a release vote requires 3 TSC members to pass.
See the documentation on the [JanusGraph release policy](https://docs.janusgraph.org/development/#release-policy) for more information.

### Finalize the Release

*This is the point of no return.* After releasing artifacts to Maven
Central and pushing history to the public GitHub repo, the release
can't be canceled. 

Update the `STRUCTOR_LATEST_TAG` variable in the settings of the JanusGraph project 
in the Travis CI web interface and ensure that 
the newest version is the actually latest version in our documentation, 
for example, you release `v0.3.3` but `v0.4.0` is already released,
`STRUCTOR_LATEST_TAG` variable should be `v0.4.0`.

### Release the staging repository

When the vote is complete and successful, it is time to finalize the release.
Log into Sonatype and select the [staging repository](https://oss.sonatype.org/#stagingRepositories) that was closed previously.
Once the release is selected click `Release`.
The staging directory should be automatically deleted after the release is completed.
The release artifacts will be synched over to Maven Central in about 2 hours.

### Update from pre-release to release

Edit the release on GitHub and uncheck the box for pre-release.
Verify that on the release page that the release is now labeled "Latest Release".

### Publish the documentation

Merge the pull request for the release documentation on docs.janusgraph.org.
It takes about a minute for the documentation site to get published.

### Prepare the next snapshot release

```Shell
 mvn versions:set -DnewVersion=0.3.0-SNAPSHOT -DgenerateBackupPoms=false
```

Restore the `<scm>` to `<tag>HEAD</tag>` in the root `pom.xml` file.

Create an issue to initialize the next SNAPSHOT release.

Open a pull request with the `pom.xml` updates as a fix for that issue and also
update `snapshot_version` in the `mkdocs.yml`. 

### Announce the new release

Once it has been verified that the artifacts have populated in Nexus the release manager should announce the release in [janusgraph-user](https://lists.lfaidata.foundation/g/janusgraph-users), [janusgraph-dev](https://lists.lfaidata.foundation/g/janusgraph-dev) and [janusgraph-announce](https://lists.lfaidata.foundation/g/janusgraph-announce).
Here is an [example announcement thread](https://groups.google.com/forum/#!searchin/janusgraph-users/ANNOUNCE%7Csort:date/janusgraph-users/nAgHta8mw-A/pOsd8qpqBgAJ).

### Document lessons learned

If you find anything incorrect or incomplete in this document during the process of preparing a release it is important to feed that information back into the community.
Fine details should be added to this document, while high level items could merit updates to the [releasing policy.](https://docs.janusgraph.org/development/#release-policy)
