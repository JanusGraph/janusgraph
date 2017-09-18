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

import java.util.List;
import java.util.stream.Collectors;

import lombok.ToString;
import org.janusgraph.codepipelines.model.ParallelBuildAction;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.codebuild.CodeBuildClient;
import software.amazon.awssdk.services.codebuild.model.ArtifactPackaging;
import software.amazon.awssdk.services.codebuild.model.ArtifactsType;
import software.amazon.awssdk.services.codebuild.model.ComputeType;
import software.amazon.awssdk.services.codebuild.model.CreateProjectRequest;
import software.amazon.awssdk.services.codebuild.model.CreateProjectResponse;
import software.amazon.awssdk.services.codebuild.model.EnvironmentType;
import software.amazon.awssdk.services.codebuild.model.EnvironmentVariable;
import software.amazon.awssdk.services.codebuild.model.Project;
import software.amazon.awssdk.services.codebuild.model.ProjectArtifacts;
import software.amazon.awssdk.services.codebuild.model.ProjectEnvironment;
import software.amazon.awssdk.services.codebuild.model.ProjectSource;
import software.amazon.awssdk.services.codebuild.model.SourceType;
import software.amazon.awssdk.services.codepipeline.CodePipelineClient;
import software.amazon.awssdk.services.codepipeline.model.ActionCategory;
import software.amazon.awssdk.services.codepipeline.model.ActionDeclaration;
import software.amazon.awssdk.services.codepipeline.model.ActionOwner;
import software.amazon.awssdk.services.codepipeline.model.ActionTypeId;
import software.amazon.awssdk.services.codepipeline.model.ArtifactStore;
import software.amazon.awssdk.services.codepipeline.model.ArtifactStoreType;
import software.amazon.awssdk.services.codepipeline.model.CreatePipelineRequest;
import software.amazon.awssdk.services.codepipeline.model.CreatePipelineResponse;
import software.amazon.awssdk.services.codepipeline.model.InputArtifact;
import software.amazon.awssdk.services.codepipeline.model.OutputArtifact;
import software.amazon.awssdk.services.codepipeline.model.PipelineDeclaration;
import software.amazon.awssdk.services.codepipeline.model.StageDeclaration;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketLocationConstraint;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutBucketTaggingRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;

/**
 * @author Alexander Patrikalakis
 */
@Slf4j
@Builder
@RequiredArgsConstructor
@ToString
class AwsCodePipelinesLogic {
    private final S3Client s3;
    private final String s3Bucket;
    private final BucketLocationConstraint s3BucketLocationConstraint;
    private final String profileName;
    private final String pipelineName;
    private final String defaultComputeImage;
    private final ComputeType defaultComputeType;
    private final boolean defaultPrivilegedMode;
    private final String codeBuildServiceRoleArn;
    private final CodePipelineClient codePipeline;
    private final CodeBuildClient codeBuild;
    private final String codePipelineRoleArn;
    private final String githubToken;
    private final String githubOwner;
    private final String githubRepo;
    private final String githubBranch;
    private final String computeImage;
    private final List<ParallelBuildAction> parallelBuildActions;
    private final List<Tag> tags;
    private final String sourceOutputArtifactName;

    private ActionDeclaration createBuildActionDeclaration(final Project project, int version) {
        return ActionDeclaration.builder()
            .name(project.name())
            .inputArtifacts(InputArtifact.builder().name(sourceOutputArtifactName).build())
            .actionTypeId(ActionTypeId.builder()
                .category(ActionCategory.Build)
                .owner(ActionOwner.AWS)
                .provider("CodeBuild")
                .version(Integer.toString(version)).build()
            )
            .outputArtifacts(OutputArtifact.builder().name(project.artifacts().name()).build())
            .configuration(ImmutableMap.of("ProjectName", project.name()))
            .runOrder(1).build(); //all the builds will run in parallel
    }

    private StageDeclaration createBuildStageDeclaration(final List<Project> createdProjects, final int version) {
        return StageDeclaration.builder()
            .name("Build")
            .actions(createdProjects.stream()
                .map(action -> createBuildActionDeclaration(action, version))
                .collect(Collectors.toList())).build();
    }

    private StageDeclaration createSourceStageDeclaration(int version) {
        Preconditions.checkArgument(version > 0);
        return StageDeclaration.builder().name("Source")
            .actions(ActionDeclaration.builder().name(pipelineName)
                .actionTypeId(ActionTypeId.builder()
                    .category(ActionCategory.Source)
                    .owner(ActionOwner.ThirdParty)
                    .provider("GitHub")
                    .version(Integer.toString(version)).build())
                .outputArtifacts(OutputArtifact.builder().name(sourceOutputArtifactName).build())
                .configuration(ImmutableMap.of(
                    "Owner", githubOwner,
                    "Repo", githubRepo,
                    "Branch", githubBranch,
                    "OAuthToken", githubToken))
                .runOrder(1)
                .build()
            )
            .build();
    }

    private CreateProjectRequest createCodeBuildProjectRequest(final ParallelBuildAction action) {
        log.info("Creating CodeBuild project for " + action.getName());
        Preconditions.checkArgument(action.getTimeout() <= 480 && action.getTimeout() > 0,
            "timeoutInMinutes must be greater than zero and less than or equal to 8 hours");
        return CreateProjectRequest.builder()
            .name(pipelineName + "-" + action.getName())
            .serviceRole(codeBuildServiceRoleArn)
            //.withTags(null) //TODO fix
            .artifacts(ProjectArtifacts.builder().packaging(ArtifactPackaging.NONE)
                .type(ArtifactsType.CODEPIPELINE)
                .name(action.getName() + "-artifacts").build())
            .timeoutInMinutes(action.getTimeout())
            .environment(ProjectEnvironment.builder()
                .computeType(action.getComputeType().orElse(defaultComputeType))
                .image(action.getComputeImage().orElse(defaultComputeImage)) //pull down into action if necessary later
                .type(EnvironmentType.LINUX_CONTAINER)
                .privilegedMode(action.getPrivilegedMode().orElse(defaultPrivilegedMode))
                .environmentVariables(action.getEnv().stream()
                    .map(e -> EnvironmentVariable.builder().name(e.getName()).value(e.getValue()).build())
                    .collect(Collectors.toList())).build())
            .source(ProjectSource.builder().type(SourceType.CODEPIPELINE).build()).build();
    }

    /**
     * This method assures that the S3 bucket is available,
     * creates individual build projects for each module defined in the input YAML file
     * creates the source and build stages of the pipeline, and finally creates the pipeline.
     */
    void run() {
        assureS3Bucket();
        final List<Project> buildProjects = createBuildProjects();
        final int version = 1;
        final StageDeclaration sourceStage = createSourceStageDeclaration(version);
        final StageDeclaration buildStage = createBuildStageDeclaration(buildProjects, version);
        final CreatePipelineResponse pipeline = codePipeline.createPipeline(
            CreatePipelineRequest.builder().pipeline(PipelineDeclaration.builder()
                .roleArn(codePipelineRoleArn)
                .name(pipelineName)
                .version(Integer.MAX_VALUE)
                .stages(sourceStage, buildStage)
                .artifactStore(ArtifactStore.builder().type(ArtifactStoreType.S3).location(s3Bucket).build()).build()).build());

        Preconditions.checkNotNull(pipeline, "created pipeline was null");
    }

    private void assureS3Bucket() {
        try {
            s3.createBucket(CreateBucketRequest.builder()
                .createBucketConfiguration(CreateBucketConfiguration.builder().locationConstraint(s3BucketLocationConstraint).build())
                .bucket(s3Bucket)
                .build());
            s3.putBucketTagging(PutBucketTaggingRequest.builder().bucket(s3Bucket).tagging(Tagging.builder().tagSet(tags).build()).build());
            log.info("Created bucket " + s3Bucket);
        } catch (S3Exception e) {
            if (e.getErrorCode().equals("BucketAlreadyOwnedByYou")) {
                log.info("Bucket " + s3Bucket + " in region already owned by you");
            } else {
                throw new IllegalArgumentException("Unable to create/configure bucket", e);
            }
        }
    }

    private List<Project> createBuildProjects() {
        return parallelBuildActions.stream()
            .map(this::createCodeBuildProjectRequest)
            .peek(project -> log.info(project.toString()))
            .map(codeBuild::createProject)
            .map(CreateProjectResponse::project)
            .collect(Collectors.toList());
    }
}
