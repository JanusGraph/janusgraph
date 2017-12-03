// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package org.janusgraph.codepipelines;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableList;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.janusgraph.codepipelines.model.PipelineDefinitions;

import software.amazon.awssdk.auth.AwsCredentialsProvider;
import software.amazon.awssdk.auth.ProfileCredentialsProvider;
import software.amazon.awssdk.client.builder.ClientHttpConfiguration;
import software.amazon.awssdk.http.apache.ApacheSdkHttpClientFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.codebuild.CodeBuildClient;
import software.amazon.awssdk.services.codepipeline.CodePipelineClient;
import software.amazon.awssdk.services.iam.model.IAMException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketLocationConstraint;
import software.amazon.awssdk.services.s3.model.Tag;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;

/**
 * This class orchestrates some CodePipelines stacks to run JanusGraph release tests in parallel.
 * @author Alexander Patrikalakis
 */
@Slf4j
@RequiredArgsConstructor
public class AwsCodePipelinesCi {

    private static final Option REGION_OPTION = createRequiredOneArgOption("region", "AWS region to create build stack");
    private static final Option BUCKET_OPTION = createRequiredOneArgOption("bucket", "AWS S3 bucket to store artifacts");
    private static final Option CODEPIPELINE_ROLE_ARN_OPTION =
        createRequiredOneArgOption("codepipeline-role-arn", "ARN of IAM role for CodePipeline to use");
    private static final Option GITHUB_OWNER_OPTION =
        createRequiredOneArgOption("github-owner", "GitHub owner (organization) of the repository to build");
    private static final Option GITHUB_REPO_OPTION = createRequiredOneArgOption("github-repo", "GitHub repository to build");
    private static final Option GITHUB_BRANCH_OPTION = createRequiredOneArgOption("github-branch", "GitHub repository branch to build");
    private static final Option GITHUB_TOKEN_OPTION =
        createRequiredOneArgOption("github-token", "GitHub personal access token for AWS CodeBuild to use to build");
    private static final Option PROFILE_OPTION =
        createRequiredOneArgOption("profile", "AWS credential profile to use to create the stack");
    private static final Option PIPELINES_JSON_OPTION =
        createRequiredOneArgOption("pipelines","Path to JSON file containing abbreviated pipeline definitions");
    private static final Option CODE_BUILD_SERVICE_ROLE_ARN_OPTION =
        createRequiredOneArgOption("codebuild-role-arn",
            "ARN of the service role for CodeBuild (http://docs.aws.amazon.com/codebuild/latest/userguide/setting-up.html#setting-up-service-role)");
    private static final Option CODE_BUILD_COMPUTE_IMAGE = createOptionalOneArgOption("codebuild-compute-image",
        "Compute image to use for CodeBuild");

    private static final List<Option> OPTIONS = ImmutableList.of(REGION_OPTION,
        BUCKET_OPTION, CODEPIPELINE_ROLE_ARN_OPTION, GITHUB_OWNER_OPTION, GITHUB_REPO_OPTION, GITHUB_BRANCH_OPTION, GITHUB_TOKEN_OPTION,
        PROFILE_OPTION, PIPELINES_JSON_OPTION, CODE_BUILD_SERVICE_ROLE_ARN_OPTION, CODE_BUILD_COMPUTE_IMAGE);
    private static final boolean HAS_ARG = true;
    private static final String NULL_SHORT_OPT = null;
    private static final boolean REQUIRED_OPTION = true;
    private static final boolean NOT_REQUIRED_OPTION = false;

    private final CommandLine cmd;

    private void run() throws IOException {
        final File file = new File(getOptionValue(PIPELINES_JSON_OPTION));
        final Region region = Region.of(getOptionValue(REGION_OPTION));
        final AwsCredentialsProvider provider = ProfileCredentialsProvider.builder()
            .profileName(getOptionValue(PROFILE_OPTION)).build();

        final ClientHttpConfiguration http = ClientHttpConfiguration.builder()
            .httpClient(ApacheSdkHttpClientFactory.builder() //consider netty some other time
                .socketTimeout(Duration.ofSeconds(10))
                .connectionTimeout(Duration.ofMillis(750))
                .build().createHttpClient())
            .build();

        final AwsCodePipelinesLogic.AwsCodePipelinesLogicBuilder builder = AwsCodePipelinesLogic.builder()
            .githubToken(getOptionValue(GITHUB_TOKEN_OPTION))
            .githubOwner(getOptionValue(GITHUB_OWNER_OPTION))
            .githubRepo(getOptionValue(GITHUB_REPO_OPTION))
            .githubBranch(getOptionValue(GITHUB_BRANCH_OPTION))
            .codeBuildServiceRoleArn(getOptionValue(CODE_BUILD_SERVICE_ROLE_ARN_OPTION))
            .codePipelineRoleArn(getOptionValue(CODEPIPELINE_ROLE_ARN_OPTION))
            .s3Bucket(getOptionValue(BUCKET_OPTION))
            .s3BucketLocationConstraint(BucketLocationConstraint.fromValue(region.value()))
            .s3(S3Client.builder().httpConfiguration(http).region(region).credentialsProvider(provider).build())
            .codeBuild(CodeBuildClient.builder().httpConfiguration(http).region(region).credentialsProvider(provider).build())
            .codePipeline(CodePipelineClient.builder().httpConfiguration(http).region(region).credentialsProvider(provider).build())
            ;

        final Tag timeTag = Tag.builder().key("date").value(Long.toString(System.currentTimeMillis())).build();
        final PipelineDefinitions definitions = new ObjectMapper(new YAMLFactory()).readValue(file, PipelineDefinitions.class);
        definitions.getPipelines().stream()
            .map(def -> builder
                    .pipelineName(def.getName())
                    .sourceOutputArtifactName(def.getName() + "Source")
                    .parallelBuildActions(def.getParallelBuildActions())
                    .defaultComputeImage(definitions.getDefaultComputeImage())
                    .defaultComputeType(definitions.getDefaultComputeType())
                    .defaultPrivilegedMode(definitions.isDefaultPrivilegedMode())
                    .tags(ImmutableList.of(Tag.builder().key("project").value(def.getName()).build(), timeTag))
                    .build())
            .forEach(AwsCodePipelinesLogic::run);
    }

    public static void main(String[] args) {
        int status = 0;

        try {
            final Options options = new Options();
            OPTIONS.forEach(options::addOption);
            new AwsCodePipelinesCi(new DefaultParser().parse(options, args)).run();
        } catch (ParseException | IllegalArgumentException e) {
            log.error(e.getMessage(), e);
            status = 22; //EINVAL
        } catch (IAMException e) {
            log.error(e.getMessage(), e);
            status = 1;  //EPERM
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            status = 11; //EAGAIN
        }
        System.exit(status);
    }

    private String getOptionValue(Option option) {
        return cmd.getOptionValue(option.getLongOpt());
    }

    private static Option createRequiredOneArgOption(final String longOpt, final String description) {
        return createOneArgOption(longOpt, description, REQUIRED_OPTION);
    }

    private static Option createOptionalOneArgOption(final String longOpt, final String description) {
        return createOneArgOption(longOpt, description, NOT_REQUIRED_OPTION);
    }

    private static Option createOneArgOption(final String longOpt, final String description, final boolean required) {
        final Option o = new Option(NULL_SHORT_OPT, longOpt, HAS_ARG, description);
        o.setRequired(required);
        o.setArgs(1);
        return o;
    }
}
