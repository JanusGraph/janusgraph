package org.janusgraph.codepipelines;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.assertj.core.util.Lists;
import org.janusgraph.codepipelines.model.ParallelBuildAction;
import org.janusgraph.codepipelines.model.PipelineDefinition;
import org.janusgraph.codepipelines.model.PipelineDefinitions;
import software.amazon.awssdk.auth.AwsCredentialsProvider;
import software.amazon.awssdk.auth.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.codebuild.CodeBuildClient;
import software.amazon.awssdk.services.codebuild.model.ArtifactPackaging;
import software.amazon.awssdk.services.codebuild.model.ArtifactsType;
import software.amazon.awssdk.services.codebuild.model.ComputeType;
import software.amazon.awssdk.services.codebuild.model.CreateProjectRequest;
import software.amazon.awssdk.services.codebuild.model.EnvironmentType;
import software.amazon.awssdk.services.codebuild.model.EnvironmentVariable;
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
import software.amazon.awssdk.services.iam.model.IAMException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketLocationConstraint;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutBucketTaggingRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class orchestrates some CodePipelines stacks to run JanusGraph release tests in parallel.
 * @author Alexander Patrikalakis
 */
@Slf4j
@ToString
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
    private static final Option CODE_BUILD_TIMEOUT_IN_MINUTES =
        createOptionalOneArgOption("codebuild-timeout-in-minutes",
            "Maximum time in minutes for a build. Must be greater than 0 and less than or equal to 480.");
    private static final String DEFAULT_TIMEOUT_IN_MINUTES = "480";
    private static final Option CODE_BUILD_COMPUTE_IMAGE = createOptionalOneArgOption("codebuild-compute-image",
        "Compute image to use for CodeBuild");
    private static final String DEFAULT_COMPUTE_IMAGE = "stephenreed/jenkins-java8-maven-git:latest";

    private final List<Tag> tags;
    private final String s3Bucket;
    private final String region;
    private final String pipelineName;
    private final String codeBuildServiceRoleArn;
    private final String sourceOutputArtifactName;
    private final S3Client s3;
    private final CodePipelineClient codePipeline;
    private final CodeBuildClient codeBuild;
    private final String codePipelineRoleArn;
    private final Map<String, ParallelBuildAction> parallelBuildActions;
    private final Map<String, String> projectNameMap;
    private final String githubToken;
    private final String githubOwner;
    private final String githubRepo;
    private final String githubBranch;
    private final String computeImage;
    private final int timeoutInMinutes;

    private AwsCodePipelinesCi(final String region, final String s3bucket, PipelineDefinition definition,
                       final String codePipelineRoleArn,
                       final String githubToken, final String profileName,
                       final String githubOwner, final String githubRepo,
                       final String githubBranch, final String codeBuildServiceRoleArn,
                       final String computeImage,
                       final int timeoutInMinutes) {
        this.s3Bucket = s3bucket;
        this.tags = Lists.newArrayList(Tag.builder().key("project").value(definition.getName()).build(),
            Tag.builder().key("date").value(Long.toString(System.currentTimeMillis())).build());
        this.region = region;
        this.pipelineName = definition.getName();
        this.codeBuildServiceRoleArn = codeBuildServiceRoleArn;
        this.sourceOutputArtifactName = pipelineName + "Source";
        this.parallelBuildActions = definition.getParallelBuildActions().stream().collect(Collectors.toMap(ParallelBuildAction::getName, Function.identity()));
        this.projectNameMap = parallelBuildActions.keySet().stream().collect(Collectors.toMap(Function.identity(), envName -> pipelineName + "-" + envName));
        this.codePipelineRoleArn = codePipelineRoleArn;
        this.githubToken = githubToken;
        this.githubOwner = githubOwner;
        this.githubRepo = githubRepo;
        this.githubBranch = githubBranch;
        this.computeImage = computeImage;
        this.timeoutInMinutes = timeoutInMinutes;

        //setup clients
        final Region regionEnum = Region.of(region);
        final AwsCredentialsProvider provider = ProfileCredentialsProvider.builder().profileName(profileName).build();
        this.s3 = S3Client.builder().region(regionEnum).credentialsProvider(provider).build();
        this.codePipeline = CodePipelineClient.builder().region(regionEnum).credentialsProvider(provider).build();
        this.codeBuild = CodeBuildClient.builder().region(regionEnum).credentialsProvider(provider).build();
    }

    private String getBuildOutputArtifactName(String action) {
        return this.projectNameMap.get(action) + "-artifacts";
    }

    private ActionDeclaration createBuildActionDeclaration(ParallelBuildAction action, int version) {
        final String actionName = action.getName();
        return ActionDeclaration.builder()
                .name(actionName)
                .inputArtifacts(InputArtifact.builder().name(sourceOutputArtifactName).build())
                .actionTypeId(ActionTypeId.builder()
                        .category(ActionCategory.Build)
                        .owner(ActionOwner.AWS)
                        .provider("CodeBuild")
                        .version(Integer.toString(version)).build()
                )
                .outputArtifacts(OutputArtifact.builder().name(getBuildOutputArtifactName(actionName)).build())
                .configuration(ImmutableMap.of("ProjectName", projectNameMap.get(actionName)))
                .runOrder(Integer.valueOf(1)).build(); //all the builds will run in parallel
    }

    private StageDeclaration createBuildStageDeclaration(int version) {
        return StageDeclaration.builder()
                .name("Build")
                .actions(parallelBuildActions.values().stream()
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

    private CreateProjectRequest createCodeBuildProjectRequest(ParallelBuildAction action,
                                                               String serviceRoleArn,
                                                               String artifactsName,
                                                               ComputeType computeType,
                                                               String computeImage,
                                                               int timeoutInMinutes) {
        log.info("Creating CodeBuild project for " + action.getName());
        Preconditions.checkArgument(timeoutInMinutes <= 480 && timeoutInMinutes > 0,
            "timeoutInMinutes must be greater than zero and less than or equal to 8 hours");
        return CreateProjectRequest.builder()
                .name(projectNameMap.get(action.getName()))
                .serviceRole(serviceRoleArn)
                //.withTags(null) //TODO fix
                .artifacts(ProjectArtifacts.builder().packaging(ArtifactPackaging.NONE).type(ArtifactsType.CODEPIPELINE).name(artifactsName).build())
                .timeoutInMinutes(timeoutInMinutes)
                .environment(ProjectEnvironment.builder()
                        .computeType(computeType)
                        .image(computeImage)
                        .type(EnvironmentType.LINUX_CONTAINER)
                        .privilegedMode(Boolean.TRUE)
                        .environmentVariables(action.getEnv().stream()
                                .map(e -> EnvironmentVariable.builder().name(e.getName()).value(e.getValue()).build())
                                .collect(Collectors.toList())).build())
                .source(ProjectSource.builder().type(SourceType.CODEPIPELINE).build()).build();
    }

    private void run() {
        try {
            s3.createBucket(CreateBucketRequest.builder()
                .createBucketConfiguration(CreateBucketConfiguration.builder().locationConstraint(BucketLocationConstraint.fromValue(region)).build())
                .bucket(s3Bucket)
                .build());
            s3.putBucketTagging(PutBucketTaggingRequest.builder().bucket(s3Bucket).tagging(Tagging.builder().tagSet(tags).build()).build());
            log.info("Created bucket " + s3Bucket + " in region " + region);
        } catch (S3Exception e) {
            if (e.getErrorCode().equals("BucketAlreadyOwnedByYou")) {
                log.info("Bucket " + s3Bucket + " in region " + region + " already owned by you");
            } else {
                throw new IllegalArgumentException("Unable to create/configure bucket", e);
            }
        }

        parallelBuildActions.entrySet().stream()
                .map(entry -> createCodeBuildProjectRequest(entry.getValue(),
                            codeBuildServiceRoleArn,
                            getBuildOutputArtifactName(entry.getKey()),
                            ComputeType.BUILD_GENERAL1_LARGE, //TODO externalize
                            computeImage,
                            timeoutInMinutes)
                )
                .peek(project -> log.info(project.toString()))
                .forEach(codeBuild::createProject);

        final int version = 1;
        final StageDeclaration sourceStage = createSourceStageDeclaration(version);
        final StageDeclaration buildStage = createBuildStageDeclaration(version);
        final CreatePipelineResponse pipeline = codePipeline.createPipeline(
            CreatePipelineRequest.builder().pipeline(PipelineDeclaration.builder()
                        .roleArn(codePipelineRoleArn)
                        .name(pipelineName)
                        .version(Integer.MAX_VALUE)
                        .stages(sourceStage, buildStage)
                        .artifactStore(ArtifactStore.builder().type(ArtifactStoreType.S3).location(s3Bucket).build()).build()).build());

        Preconditions.checkNotNull(pipeline, "created pipeline was null");
    }

    private static Option createRequiredOneArgOption(final String longOpt, final String description) {
        final Option o = new Option(null /*shortOpt*/, longOpt, true /*hasArg*/, description);
        o.setRequired(true);
        o.setArgs(1);
        return o;
    }

    private static Option createOptionalOneArgOption(final String longOpt, final String description) {
        final Option o = new Option(null /*shortOpt*/, longOpt, true /*hasArg*/, description);
        o.setRequired(false);
        o.setArgs(1);
        return o;
    }

    public static void main(String... args) {
        int status = 0;
        try {
            final Options options = new Options();
            options.addOption(REGION_OPTION);
            options.addOption(BUCKET_OPTION);
            options.addOption(CODEPIPELINE_ROLE_ARN_OPTION);
            options.addOption(GITHUB_OWNER_OPTION);
            options.addOption(GITHUB_REPO_OPTION);
            options.addOption(GITHUB_BRANCH_OPTION);
            options.addOption(GITHUB_TOKEN_OPTION);
            options.addOption(PROFILE_OPTION);
            options.addOption(PIPELINES_JSON_OPTION);
            options.addOption(CODE_BUILD_SERVICE_ROLE_ARN_OPTION);
            options.addOption(CODE_BUILD_COMPUTE_IMAGE);
            options.addOption(CODE_BUILD_TIMEOUT_IN_MINUTES);
            final CommandLineParser parser = new DefaultParser();
            final CommandLine cmd = parser.parse(options, args);

            //TODO ConstructorProperties was introduced in jacskon 2.7, use it
            final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            final PipelineDefinitions definitions = mapper.readValue(new File(cmd.getOptionValue(PIPELINES_JSON_OPTION.getLongOpt())), PipelineDefinitions.class);
            definitions.getPipelines().forEach(definition -> {
                final AwsCodePipelinesCi ci = new AwsCodePipelinesCi(
                    cmd.getOptionValue(REGION_OPTION.getLongOpt()),
                    cmd.getOptionValue(BUCKET_OPTION.getLongOpt()),
                    definition,
                    cmd.getOptionValue(CODEPIPELINE_ROLE_ARN_OPTION.getLongOpt()),
                    cmd.getOptionValue(GITHUB_TOKEN_OPTION.getLongOpt()),
                    cmd.getOptionValue(PROFILE_OPTION.getLongOpt()),
                    cmd.getOptionValue(GITHUB_OWNER_OPTION.getLongOpt()),
                    cmd.getOptionValue(GITHUB_REPO_OPTION.getLongOpt()),
                    cmd.getOptionValue(GITHUB_BRANCH_OPTION.getLongOpt()),
                    cmd.getOptionValue(CODE_BUILD_SERVICE_ROLE_ARN_OPTION.getLongOpt()),
                    Optional.ofNullable(cmd.getOptionValue(CODE_BUILD_COMPUTE_IMAGE.getLongOpt())).orElse(DEFAULT_COMPUTE_IMAGE),
                    Integer.valueOf(Optional.ofNullable(cmd.getOptionValue(CODE_BUILD_TIMEOUT_IN_MINUTES.getLongOpt())).orElse(DEFAULT_TIMEOUT_IN_MINUTES)));
                ci.run();
            });
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
}
