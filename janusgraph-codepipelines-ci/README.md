# JanusGraph CodePipelines CI
CodePipelines CI is a mechanism JanusGraph can use to do release testing in massively
parallel fashion (to the extent of AWS CodePipelines and CodeBuild service limits).

## Prerequisites
This procedure requires you to have an AWS account and a GitHub account.
It also requires you to create two service roles in IAM: one for CodePipeline and
one for CodeBuild. Finally, you need to have the [AWS CLI](https://aws.amazon.com/cli/) installed and on your path.

1. Get a personal access token from [GitHub](https://github.com/settings/tokens) with `repo` and `admin:repo_hook` scopes.
The `repo` scope is used to push the latest updates on the branch selected below. The `admin:repo_hook` scope is for
setting up the post-commit hook on GitHub programmatically.
2. Navigate to the [AWS Console](https://console.aws.amazon.com) and
[create an IAM User](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html) with the following managed
policies: AmazonS3FullAccess, AWSCodePipelineFullAccess, AWSCodeBuildAdminAccess. This user should be created for
__Programmatic access__.
3. For this user, create security credentials and then register them in the `code-pipelines` profile on your computer
with `aws configure --profile code-pipelines`. Create this profile with a default region that
[supports CodePipeline and CodeBuild](https://aws.amazon.com/about-aws/global-infrastructure/regional-product-services/)
and a default [output format](https://docs.aws.amazon.com/cli/latest/userguide/controlling-output.html#controlling-output-format)
of your choice (`json`, `text`, and `table` are available).
4. Create an IAM policy for CodeBuild and associate it to a new service role as described
[here](https://docs.aws.amazon.com/codebuild/latest/userguide/setting-up.html#setting-up-service-role). Select the
__AWS CodeBuild__ role type. The documentation linked here says the CodeBuild service role type is not available, but
it actually is.
5. Create an IAM policy for CodePipelines and associate it to a new service role as described
[here](https://docs.aws.amazon.com/codepipeline/latest/userguide/iam-identity-based-access-control.html#view-default-service-role-policy).
Select the __Amazon EC2__ role type as CodePipeline is not yet available on this page. Edit the new role's trust
relationship and replace the principal `ec2.amazonaws.com` with `codepipeline.amazonaws.com`.

## Setup environment
Follow the steps below to prepare to create a test pipeline and run tests. First, clean up and set some constant
environment variables.

```bash
mvn clean package
#constants
export AWS_PROFILE_NAME='code-pipelines'
export AWS_ACCOUNT_NUMBER=`aws sts get-caller-identity --profile ${AWS_PROFILE_NAME} --output text | cut -f1`
```

Second, configure the parameters below to customize your test stack.

```bash
#GitHub personal access token you created in step 1
export GITHUB_TOKEN=''
#GitHub organization or user name of the repository to build
export GITHUB_USERNAME=''
#GitHub repository name to use for builds
export GITHUB_REPOSITORY=''
#GitHub branch in the above repository to use for builds
export GITHUB_BRANCH=''
#Name of the AWS CodeBuild service role you created in step 4
export AWS_CODEBUILD_ROLE=''
#Name of the AWS CodePipeline role you created in step 6
export AWS_CODEPIPELINE_ROLE=''
#Region you want to set up your test stack in
export AWS_REGION_NAME=''
#Name of bucket you want to use or create and use for storing your build artifacts.
export AWS_S3_BUCKET_NAME=''
#Name of pipeline configuration file to use
export PIPELINE_CONFIGURATION=''
```

## Create test pipeline
This tool uses templates defined in YAML files to configure parallel builds in AWS CodePipeline. By default,
you can have up to [five parallel actions per stage](https://docs.aws.amazon.com/codepipeline/latest/userguide/limits.html)
in a pipeline and up to [twenty parallel builds](https://docs.aws.amazon.com/codebuild/latest/userguide/limits.html#limits-builds)
per region per account. If you need more, you can
[request to be whitelisted](https://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html) for more.

Here is an example of a file that defines two pipelines with parallel build actions.

```yaml
defaultComputeImage: maven:3-jdk-8
defaultComputeType: BUILD_GENERAL1_LARGE
defaultPrivilegedMode: true
pipelines:
- name: j1
  parallelBuildActions:
  - name: bdb
    timeout: 30
    env:
    - name: MODULE
      value: janusgraph-berkeleyje
  - name: h-l-s
    timeout: 42
    env:
    - name: MODULE
      value: janusgraph-hadoop,janusgraph-lucene,janusgraph-solr
  - name: cassandra
    timeout: 220
    env:
    - name: MODULE
      value: janusgraph-cassandra
  - name: test
    timeout: 43
    env:
    - name: MODULE
      value: janusgraph-test
- name: j2
  parallelBuildActions:
  - name: hbase098
    timeout: 130
    env:
    - name: MODULE
      value: janusgraph-hbase-parent/janusgraph-hbase-098
  - name: hbase10
    timeout: 79
    env:
    - name: MODULE
      value: janusgraph-hbase-parent/janusgraph-hbase-10
  - name: es
    timeout: 100
    env:
    - name: MODULE
      value: janusgraph-es
  - name: cql
    timeout: 134
    env:
    - name: MODULE
      value: janusgraph-cql
```
The first pipeline is called `j1` and the second pipeline is called `j2`. Each of these pipelines have four
parallel build actions defined. Each build action is keyed at the action name and includes the environment
variables to be passed CodeBuild and used in the buildspec.yml file.

To kick off the regular and TinkerPop tests. Use `export PIPELINE_CONFIGURATION=pipe.yml` for regular tests
and `export PIPELINE_CONFIGURATION=tp-pipe.yml` for TinkerPop tests.

```bash
java -jar target/janusgraph-codepipelines-ci-0.2.0-SNAPSHOT.jar \
--region ${AWS_REGION_NAME} \
--bucket ${AWS_S3_BUCKET_NAME} \
--github-owner ${GITHUB_USERNAME} \
--github-repo ${GITHUB_REPOSITORY} \
--github-branch ${GITHUB_BRANCH} \
--profile ${AWS_PROFILE_NAME} \
--codebuild-role-arn arn:aws:iam::${AWS_ACCOUNT_NUMBER}:role/${AWS_CODEBUILD_ROLE} \
--codepipeline-role-arn arn:aws:iam::${AWS_ACCOUNT_NUMBER}:role/${AWS_CODEPIPELINE_ROLE} \
--github-token ${GITHUB_TOKEN} \
--pipelines ${PIPELINE_CONFIGURATION}
```

Navigate to the [AWS Console](https://console.aws.amazon.com/codepipeline) and check on the status of your pipeline to see status.

## Cleaning up
After you follow the steps above, you will end up with resources in the following services:
1. Build artifacts and source code zips in S3
2. CloudWatch logs
3. CodeBuild builds
4. CodePipeline pipelines, stages, and actions
5. IAM policies, roles and an IAM user
6. A GitHub personal access token

You can delete these resources after you are finished running your tests.
