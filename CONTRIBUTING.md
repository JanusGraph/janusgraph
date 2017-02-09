# Contributing to JanusGraph

## Sign the CLA

To sign the JanusGraph CLA, please follow [the
instructions in `janusgraph/legal`](https://github.com/JanusGraph/legal).

## Fork the repository on GitHub

Click on the [fork](https://github.com/JanusGraph/janusgraph/fork) button at the
upper-right of the repository to create a fork of the repository in your own
account.

## Clone your fork locally

To fork the repo locally, use one of the following methods:

  * if you have set up and are using [SSH keys](https://help.github.com/articles/generating-an-ssh-key/), run:

    ```bash
    $ git clone git@github.com:$USER/janusgraph
    ```

  * otherwise, to use HTTPS transfer, run:

    ```bash
    $ git clone https://github.com/$USER/janusgraph
    ```

Add the original repo to synchronize new changes:

```bash
$ git remote add upstream git@github.com:JanusGraph/janusgraph
```

Do not contribute to `master` from our own fork's `master` branch.

Also, do not push your working, in-progress, or PR branches to
`JanusGraph/janusgraph`; instead, use your own fork.

You can avoid pushing to upstream by accident by setting it as follows:

```bash
$ git remote set-url --push upstream do-not-push
```

## Configure your repo to match the CLA

Be sure to use the same name and email in your Git commits as in your signed CLA
to make sure that the automated verification process correctly matches it.

To configure them, use the following commands

```bash
$ cd janusgraph
$ git config user.name "My Name"
$ git config user.email "my-email@example.com"
```

> Note: if you forget to use the right email address the first time, and
> @janusgraph-bot adds the `[cla: no]` label to your PR, you can fix it in-place
> while reusing your existing PR as follows:
>
> * set your name and email as per above
> * run the following commands:
>
>   ```bash
>   $ git commit --amend -s --reset-author
>   $ git push -f
>   ```

## Create a new branch

Do not develop on the `master` branch: feature branches are intended to be
light-weight and deleted after being merged to upstream, but you should not
delete your `master` branch, so don't use it for development.

Instead, update your `master` branch and create a new branch for development:

```bash
$ git checkout master
$ git pull --ff-only upstream master
$ git checkout -b my-new-feature
```

## Develop and test your changes

Make that feature or bugfix awesome!

## Commit changes and sign the Developer Certificate of Origin

The Linux Foundation requires that each contributor to its projects signs the
[Developer Certificate of Origin](http://developercertificate.org), also
available in this repo as [`DCO.txt`](DCO.txt). To sign it, just use the `commit
-s` command:

```bash
$ git add [...files...]
$ git commit -s
```

> Note: if you forget to sign your commit, you can amend it and fix the PR
> in-place as follows:
>
> ```bash
> $ git commit --amend -s
> $ git push -f
> ```

You can also create a convenient alias to automatically sign each commit in this
repository to avoid forgetting it:

```bash
$ git config alias.ci 'commit -s'
```

With this alias, from now on, you can just run:

```bash
$ git ci
```

to commit files, and it will automatically append the `-s` switch to ensure that
you've signed the contribution.

> Note: as it is an alias, you can still append flags to the commad line, e.g.,
> `git ci -v` will get you a diff of your commit while writing your commit
> message.

## Push your changes to your GitHub fork

```bash
$ git push
```

If this is the first time you are pushing this branch, depending on your
settings, `git` may tell you to re-run that command with additional flags and/or
parameters. If so, copy-paste and run that command.

After you do this once for this branch, you can use `git push` to add additional
changes.

## Open a Pull Request

Go to the [JanusGraph repository](https://github.com/JanusGraph/janusgraph) and
you should see that it will offer you a chance to compare your recently-pushed
branch to the current `master` of JanusGraph and subit a PR at the same time.

## Code review

If you have signed the CLA, and it was processed and acknowledged on the email
thread, you should get a `[cla: yes]` label added to your PR by our bot,
@janusgraph-bot, during the course of the review.

If you see a `[cla: no]` label attached, there will be an explanation of what
went wrong, but generally, you should make sure that your CLA and your name,
email, and GitHub usernames all match, for all of the commits in your PR.

If you are still not sure what's wrong, reach out to
janusgraph-cla@googlegroups.com with further questions.
