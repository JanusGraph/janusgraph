# Releasing JanusGraph

There are two possible ways of making a JanusGraph release:
- **Automatic** - Partially automated release via GitHub actions. This is a preferred way of making a release.
- **Manual** - Fully manual release. This is a deprecated way of making a release. This way should only be used if 
there is a problem of using GitHub actions, or you need to compile `jar` artifacts via a custom version of Java which
isn't available in `setup-java` GitHub action.

## General prerequisites: Sonatype account

The release artifacts will be deployed into Sonatype OSS with the Maven deploy plugin.
You must have an account with Sonatype, and it must be associated with the JanusGraph org.
See the relevant issue [here.](https://issues.sonatype.org/browse/OSSRH-28274)

# Release Checklist

* [ ] Start up new `[DISCUSS]` thread on janusgraph-dev with suggestions on what should be included in the release and target date
* [ ] Make sure all PRs and issues added since last release are associated to the milestone
* [ ] Complete all items associated with milestone or move outstanding items to a new milestone if necessary
* [ ] Create Pull Requests with version updates
* [ ] Validate all changes have been merged upstream
* [ ] Push a release tag
* [ ] **For Manual release flow only** Run Deploy to create [Sonatype](https://oss.sonatype.org/#welcome) staging repository and Generate artifacts for release page
* [ ] **For Manual release flow only** Create a draft release and upload artifacts to it
* [ ] Update release description of draft release
* [ ] Close the staging repository in [Sonatype](https://oss.sonatype.org/#stagingRepositories)
* [ ] Convert draft release to a public pre-release
* [ ] Create `[VOTE]` [janusgraph-dev](https://lists.lfaidata.foundation/g/janusgraph-dev) vote thread and [get required votes of 3 TSC members](https://www.apache.org/foundation/voting.html)
* [ ] Release the staging repository in Sonatype
* [ ] Remove pre-release label on the [releases page](https://github.com/JanusGraph/janusgraph/releases/)
* [ ] Announce the new release
* [ ] Prepare the next SNAPSHOT release
* [ ] Document lessons learned

## Prerequisites [for Manual release flow only]

**Skip this entire section if you are using **Automatic** release flow.**

The release process has only been tested on Linux and macOS.
The following tools must be installed.

*   [gpg](https://www.gnupg.org/) - Requires a running agent.

### GPG Signing Key

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

Once your key has been created and added to the KEYS file it needs to be published to a public key server.
You can upload to multiple key servers within the pool or use an alternate if `pgp.mit.edu` is down.
[Sonatype typically uses](https://central.sonatype.org/publish/requirements/gpg/#distributing-your-public-key) the
following key servers: `keyserver.ubuntu.com`, `keys.openpgp.org`, `pgp.mit.edu`.

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
Otherwise, you will have to type in your gpg passphrase many times when prompted during the build.

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

### Create Pull Requests with version updates

Before any artifacts can be generated and vote can be made the version number will need to be updated in the pom.xml files.

Make sure the latest support release version in `.github/workflows/docker-release.yml` is correct.

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
* `.github/workflows/docker-release-tags.yml` - if you are publishing **latest** minor of major release you should also update `LATEST_RELEASE` in the file (ignoring patch version). This is temporary until the process is automated (see [issue #3905](https://github.com/JanusGraph/janusgraph/issues/3905)).

You may also need to update the following files in the main repo for any new or updated dependencies:

*   `NOTICE.txt`

#### Create a release commit

Create a release commit:
```Shell
git commit -m "JanusGraph release <version> [cql-tests] [tp-tests]" -s
```

#### Create the Pull Request

Open up pull requests for the version updates.
It is recommended to add `[tp-tests]` to the commit message so the TinkerPop test suite will run.
After the updates are approved and merged, continue on with the release process.

### Push version related tag

Create a release tag with the next command:
```Shell
git tag -a <release tag> -m ""
```
The release tag should follow the next pattern: **v.\<major\>.\<minor\>.\<patch\>**.
Example:
```Shell
git tag -a v0.3.2 -m ""
```
Push tag to JanusGraph remote repository (usually `upstream`):
```Shell
git push upstream <release tag>
```

In case of any changes into the release happens after the tag is pushed you will need to remove the tag, associate it 
with a new commit and push it again. After that proceed from the following step again:
```Shell
git tag --delete <release tag>
git push upstream :<release tag>
git tag -a <release tag> -m ""
git push upstream <release tag>
```

### Build Release Artifacts [for Manual release flow only]

**If you are using Automatic release flow skip this section.**

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
1. Install `python3` and `pip3` (the newest version of pip) 
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

#### For Automatic release flow

If you are using Automatic release flow your draft release will be created automatically, 
and it will be populated with all necessary release artifacts. Notice, you will need to wait for the `ci-publish.yml` 
action to be finished after your tag is submitted (Usually it takes about 10-15 minutes for this CI to be finished) for 
the draft release to be created.  
You will need to change the body of the draft release and replace `*MILESTONE_NUMBER*` with the correct milestone number 
associated to the release. You will also need to add `Notable new features`, `Tested Compatibility`,
`Installed versions in the Pre-Packaged Distribution`, and `Contributors` to the release body.
Use the previous releases as a template.
The versions can be copied from the changelog.
You need to manually determine and mark the first-time contributors.

It is recommended to keep the release in draft status until you're ready to start a vote.

Notice, if your release is already created due to re-submitting the tag again the body of your release will not be changed.  
Only the associated artifacts will be replaced with the new artifacts. You will also need to drop previous Sonatype staging release.  

#### For Manual release flow

While logged into GitHub go to the [JanusGraph releases page](https://github.com/JanusGraph/janusgraph/releases/) and click the `Draft a new release` button.
Supply the tag number, target branch, and title.  
For the description use the previous releases as a template and update the tested versions accordingly.  
All the artifacts that were created and moved to `~/jg-staging/` in the previous step need to be added to the draft release.  
Be sure to mark it as `pre-release`.  
It is recommended and keep the release in draft until you're ready to start a vote.  
In addition to the artifacts in `~/jg-staging/` a `KEYS` file must also be added to the release.  

### Close the staging repository

Log into [Sonatype](https://oss.sonatype.org/#welcome) and select Staging Repositories under Build Promotion.
If you recently uploaded you can easily find your staged release by doing a descending sort.
Verify that the contents look complete and then check the release before clicking close.

This step will run verification on all the artifacts.
In particular, this will verify that all the artifacts have a valid GPG signature.
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

### Release the staging repository

When the vote is complete and successful, it is time to finalize the release.  
Log into Sonatype and select the [staging repository](https://oss.sonatype.org/#stagingRepositories) that was closed previously.  
Once the release is selected click `Release`.  
The staging directory should be automatically deleted after the release is completed.  
The release artifacts will be synched over to Maven Central in about 2 hours.  

### Create a new branch if needed

In case the release was happening in `master` branch (which is used for `latest` minor or major version), 
you need to create a new branch in the format `v${major_version}.${minor_version}` (the patch version is ignored).
Also, if it's a new major release, ensure the branch is protected by going to [Branch protection rules](https://github.com/JanusGraph/janusgraph/settings/branches) 
and creating the same protection rules as were used for `master` branch.
This step is necessary to ensure that the next commit (snapshot restoration commit) triggers documentation build process 
and the new branch name will be added into the documentation's dropdown menu as the latest release. 

### Update from pre-release to release

Edit the release on GitHub and uncheck the box for pre-release.  
Verify that on the release page that the release is now labeled "Latest Release".

### Prepare the next snapshot release

```Shell
 mvn versions:set -DnewVersion=0.3.0-SNAPSHOT -DgenerateBackupPoms=false
```

Restore the `<scm>` to `<tag>HEAD</tag>` in the root `pom.xml` file.
Also update `snapshot_version` in the `mkdocs.yml`.
These changes can be pushed with a CTR commit.

### Announce the new release

Once it has been verified that the artifacts have populated in Nexus the release manager should announce the release in [janusgraph-user](https://lists.lfaidata.foundation/g/janusgraph-users), [janusgraph-dev](https://lists.lfaidata.foundation/g/janusgraph-dev) and [janusgraph-announce](https://lists.lfaidata.foundation/g/janusgraph-announce).  
Here is an [example announcement thread](https://lists.lfaidata.foundation/g/janusgraph-dev/topic/announce_janusgraph_0_6_0/85417137).

After announcing the release on our mailing lists, also announce it on Twitter and Discord.
Past announcements can also be used there as an inspiration.
Make sure that you *publish* the post in the Discord announcement channel.
This will allow other Discord servers to consume our announcements.

### Document lessons learned

If you find anything incorrect or incomplete in this document during the process of preparing a release it is important to feed that information back into the community.
Fine details should be added to this document, while high level items could merit updates to the [releasing policy.](https://docs.janusgraph.org/development/#release-policy)
