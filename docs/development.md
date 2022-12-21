The following sections describe the JanusGraph development process.

## Development Decisions

Many development decisions will be made during the day-to-day work of
JanusGraph contributors without any extra process overhead. However, for
larger bodies of work and significant updates and additions, the
following process shall be followed. Significant work may include, but
is not limited to:

- A major new feature or subproject, e.g., a new storage adapter
- Incrementing the version of a core dependency such as a Apache TinkerPop
- Addition or deprecation of a public API
- Internal shared data structure or API change
- Addition of a new major dependency

For these sorts of changes, contributors will:

- Create one or more issues in the GitHub issue tracker
- Start a DISCUSS thread on the [janusgraph-dev](https://lists.lfaidata.foundation/g/janusgraph-dev)
 list where the proposed change may be discussed by committers and other community members
- When the proposer feels it appropriate, a VOTE shall be called
- Two +1 votes are required for the change to be accepted

## Branching

For features that involve only one developer, developers will work in
their own JanusGraph forks, managing their own branches, and submitting
pull requests when ready. If multiple developers wish to collaborate on
a feature, they may request that a feature branch be created in
JanusGraph repository. If the developers are not committers, a
JanusGraph committer will be assigned as the shepherd for that branch.
The shepherd will be responsible for merging pull requests to that
branch.

### Branch Naming Conventions

All branch names hosted in the JanusGraph repository shall be prepended
with `Issue_#_`.

## Pull Requests

Users wishing to contribute to JanusGraph should fork the
[JanusGraph](https://github.com/janusgraph/janusgraph) repository and
then submit pull requests using the 
[GitHub pull request process](https://help.github.com/articles/creating-a-pull-request/).
Every pull request must be associated with an existing issue. Be sure to
include the relevant issue number in the title of your pull request,
complete the pull request template checklist, and provide a description
of what test suites were run to validate the pull request. Pull requests
commit messages should clearly describe what work was accomplished.
Intermediate work-in-progress commits should be squashed prior to pull
request submission.

### Review-Then-Commit (RTC)

Pull requests must be reviewed before being merged following a
review-then-commit (RTC) process. The JanusGraph project uses the
[GitHub review process](https://help.github.com/articles/about-pull-request-reviews/)
to review pull requests. Non-committers are welcomed and encouraged to
review pull requests. Their review will be non-binding, but can be taken
into consideration and are still very valuable community input. The
following rules apply to the voting process.

-   Approval flows
    -   2 committer approvals are required to merge a pull request
    -   If a committer submits the pull request, it has their implicit
        approval so it requires 1 additional committer approval
    -   1 committer approval followed a one week review period for
        objections at which point a lazy consensus is assumed
-   One or more -1 votes within a change request will veto the pull
    request until the noted issue(s) are addressed and the -1 vote is
    withdrawn
-   Change requests will not be considered valid without an explanation

### Commit-Then-Review (CTR)

In instances where the committer deems the full RTC process unnecessary,
a commit-then-review (CTR) process may be employed. The purpose of
invoking CTR is to reduce the burden on the committers and minimize the
turnaround time for merging trivial changes. Changes of this sort may
include:

- Documentation typo or small documentation addition
- Addition of a new test case
- Backporting approved changes into other release branches

Any commit that is made following CTR shall include the fact that it is
a CTR in the commit comments. Community members who wish to review CTRs
may subscribe to the [JanusGraph commits](https://groups.google.com/forum/#!forum/janusgraph-commits)
list so that they will see CTRs as they come through. If another
committer responds with a -1, the commit should be rolled back and the
formal RTC process should be followed.

### Enable Automatic Backporting

Before merging the pull request, it should be decided whether the contribution should be backported to other release
branches.
This should be the case for most non-breaking changes.
If the change applies to something that is not present at all on other still supported release branches, like updating
a dependency that was only introduced on `master` or code that was added / heavily modified on `master`, then
backporting cannot work or would require too much effort.
Otherwise, the appropriate labels for backporting (for example `backport/v0.6` for the branch `v0.6`) should be added
before the pull requests gets merged as that allows the backporting action to automatically handle the backporting.

### Merging of Pull Requests

A pull request is ready to be merged when it has been approved (see
[Review-Then-Commit (RTC)](#review-then-commit-rtc)).
It can either be merged manually with `git` commands or through the GitHub UI.

For pull requests that should be backported (see [Enable Automatic Backporting](#enable-automatic-backporting)), the
backporting action should create a pull request to backport the changes to other release branches after the pull
request has been merged.
The committer who merges the pull request should ensure that this automatic backporting succeeds.
It may fail in case of merge conflicts.
In that case the backporting needs to be [performed manually](#manual-backporting).

### Manual Backporting

The [Backport CLI tool](https://github.com/sqren/backport) can be used to manually backport a pull request.
This can be necessary if the automatic backporting of a pull request fails.
After installing the CLI tool, you also need to configure it with a GitHub access token.
This is explained in the README.md of the Backport CLI tool.

You can then use the tool to backport a pull request #xyz:

```bash
 backport --pr xyz
```

This will checkout the repository, apply the change from the pull request to the appropriate release branch
(like `v0.6`), and then ask you to resolve any merge conflicts.
After the merge conflicts are resolved, it will create a pull request that backports the changes.
Note that such a backporting pull request can usually be merged via [CTR](#commit-then-review-ctr) after all
automatic checks have been executed.
If non-trivial changes were necessary to resolve merge conflicts, then waiting for a review before merging the
pull request may still make sense.

It is of course also possible to use cherry-picking to apply the commits manually to a different release branch instead
of using this CLI tool.

## Release Policy

Any JanusGraph committer may propose a release. To propose a release,
simple start a new RELEASE thread on
[janusgraph-dev](https://lists.lfaidata.foundation/g/janusgraph-dev)
proposing the new release and requesting feedback on what should be
included in the release. After consensus is reached the release manager
will perform the following tasks:

-   Create a release branch so that work may continue on `master`
-   Prepare the release artifacts
-   Call a vote to approve the release on
    [janusgraph-dev](https://lists.lfaidata.foundation/g/janusgraph-dev)
-   Committers will be given 72 hours to review and vote on the release
    artifacts
-   Three +1 votes are required for a release to be approved
-   One or more -1 votes with explanation will veto the release until
    the noted issues are addressed and the -1 votes are withdrawn

## Building JanusGraph

To build JanusGraph you need [git](http://git-scm.com/) and
[Maven](http://maven.apache.org/).

1.  Clone the [JanusGraph repository from
    GitHub](https://github.com/JanusGraph/janusgraph) to a local
    directory.

2.  In that directory, execute `mvn clean install`. This will build
    JanusGraph and run the internal test suite. The internal test suite
    has no external dependencies. Note, that running all test cases
    requires a significant amount of time. To skip the tests when
    building JanusGraph, execute `mvn clean install -DskipTests`

3.  For comprehensive test coverage, execute
    `mvn clean test -P comprehensive`. This will run additional test
    covering communication to external storage backends, performance
    tests and concurrency tests. The comprehensive test suite uses 
    HBase as external database and requires that HBase is installed. 
    Note, that running the comprehensive test suite requires a 
    significant amount of of time (&gt; 1 hour).

### FAQs

**Maven build causes dozens of "\[WARNING\] We have a duplicate…"
errors**

Make sure to use the maven-assembly-plugin when building or depending on
JanusGraph.