# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

from aws_cdk import (
    Aspects,
    Duration,
    Stack,
    CfnOutput,
    CfnParameter,
    RemovalPolicy,
    aws_kms as kms,
    aws_sns as sns,
    aws_s3 as s3,
    aws_iam as iam,
    aws_stepfunctions as stepfunctions,
    aws_sns_subscriptions as subs,
    aws_lambda as lambda_
)
from constructs import Construct
from pathlib import Path
import random
import string
from cdk_nag import ( AwsSolutionsChecks, NagSuppressions )
import secrets

class VodMigratorStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        randomStr = generateRandomString(10)

        # Define template parameters
        email = CfnParameter(self, "email", type="String",
                                description="Email address to which SNS messages will be sent.").value_as_string
        destinationBucketArn = CfnParameter(self, "destinationBucketArn",
                                type="String",
                                description="The ARN of the destination s3 bucket where content is to be downloaded. Must be in the same region as the stack deployment.",
                                default="").value_as_string

        # Create Master KMS Key for encrypting SNS
        masterKmsKey = kms.Key( self, "MasterKmsKey",
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.DESTROY
        )

        # Create SNS Topic for notifications
        notificationTopic = sns.Topic(self, "SnsNotificationTopic",
            display_name="%s subscription topic" % (construct_id),
            master_key=masterKmsKey
        )
        notificationTopic.add_subscription(subs.EmailSubscription(email))

        # Create SNS Topic Dead Letter Topic
        deadLetterTopic = sns.Topic(self, "SnsDlqTopic",
            display_name="%s lambda dead letter queue" % (construct_id),
            master_key=masterKmsKey
        )
        deadLetterTopic.add_subscription(subs.EmailSubscription(email))

        # Create Lambda Layer
        vodDownloadLambdaFunctionName = "%s-VodDownloadFunction-%s" % (construct_id, randomStr)
        vodDownloadLambdaRole = self.createVodDownloadLambdaRole(vodDownloadLambdaFunctionName, destinationBucketArn, masterKmsKey )
        vodDownloadLayer = lambda_.LayerVersion( self, "VodDownloadLayer",
                                                description="Lambda layer containing VOD Download modules",
                                                code=lambda_.Code.from_asset("vod_migrator/layer"),
                                                compatible_architectures=[lambda_.Architecture.X86_64, lambda_.Architecture.ARM_64],
                                                compatible_runtimes=[lambda_.Runtime.PYTHON_3_12]
                                            )

        # Create Lambda Function to download VODs
        vodDownloadLambda = lambda_.Function( self,
                                            "vodDownloadFunction",
                                            function_name=vodDownloadLambdaFunctionName,
                                            description="Lambda function to download VOD Assets.",
                                            code=lambda_.Code.from_asset("vod_migrator/lambda"),
                                            runtime=lambda_.Runtime.PYTHON_3_12,
                                            handler="DownloadVod.fetchStream",
                                            role=vodDownloadLambdaRole,
                                            layers=[ vodDownloadLayer ],
                                            timeout=Duration.minutes(12),   # Slightly less than the maximum 15 min to allow lambda to stop gracefully
                                            memory_size=384,
                                            dead_letter_topic=deadLetterTopic
                                        )

        # Create role for Step Function Execution
        stateMachineName = "VodMigratorStateMachine"
        stateMachineLogDest = "arn:aws:logs:%s:%s:log-group:/aws/vendedlogs/states/%s-Logs" % (self.region, self.account, stateMachineName)
        ( stepFunctionRole, stepFunctionPolicy ) = self.createStepFunctionRole( notificationTopic, vodDownloadLambda, destinationBucketArn, masterKmsKey, stateMachineLogDest )

        # Read State Machine definition file into string
        workflowDefinition = Path('vod_migrator/statemachine/vodDownloaderWorkflow.json').read_text()
        # Create State Machine
        cfn_state_machine = stepfunctions.CfnStateMachine(self, "vodDownloadStateMachine",
            role_arn=stepFunctionRole.role_arn,

            definition_string=workflowDefinition,
            definition_substitutions={
                "SNS_TOPIC": notificationTopic.topic_arn,
                "VOD_DOWNLOAD_LAMBDA": vodDownloadLambda.function_name
            },
            state_machine_name=stateMachineName,
            state_machine_type="STANDARD",
            tracing_configuration=stepfunctions.CfnStateMachine.TracingConfigurationProperty(
                enabled=True
            ),
            ## TODO: Commented out logging configuration as this is causing the following error:
            #        "Service: AWSStepFunctions; Status Code: 400; Error Code: InvalidLoggingConfiguration"
            # logging_configuration=stepfunctions.CfnStateMachine.LoggingConfigurationProperty(
            #     destinations=[
            #         stepfunctions.CfnStateMachine.LogDestinationProperty(
            #             cloud_watch_logs_log_group=stepfunctions.CfnStateMachine.CloudWatchLogsLogGroupProperty(
            #                 log_group_arn="%s:*" % stateMachineLogDest
            #             )
            #         )
            #     ],
            #     include_execution_data=False,
            #     level="ALL"
            # )
        )

        CfnOutput(self, "StateMachineArn", value=cfn_state_machine.attr_arn)


        Aspects.of(self).add(AwsSolutionsChecks())
        NagSuppressions.add_resource_suppressions (vodDownloadLambdaRole, [
            {
                "id": "AwsSolutions-IAM5",
                "reason": "Wildcard required in IAM role to write logs to CloudWatch, write to the destination path in S3 Bucket and access all MediaPackage-VOD Assets."
            }
        ], apply_to_children=True)
        NagSuppressions.add_resource_suppressions (stepFunctionPolicy, [
            {
                "id": "AwsSolutions-IAM5",
                "reason": "Wildcard required in IAM role to write logs to CloudWatch."
            }
        ])
        NagSuppressions.add_resource_suppressions (cfn_state_machine, [
            {
                "id": "AwsSolutions-SF1",
                "reason": "########## TODO ##########: Unable to configure logging on state machine using CDK. Further investigation required as this is most likely a defect."
            }
        ])


    def createVodDownloadLambdaRole(self, vodDownloadLambdaFunctionName, destinationBucketArn, masterKmsKey):
        vodDownloadLambdaRole = iam.Role(self, "VodDownloadLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
        )

        # Allow access to CloudWatch for logging
        vodDownloadLambdaRole.add_to_policy(iam.PolicyStatement(
            actions=[
                "logs:CreateLogStream",
                "logs:CreateLogGroup",
                "logs:PutLogEvents"
            ],
            resources=[
                "arn:aws:logs:%s:%s:log-group:/aws/lambda/%s" % (self.region, self.account, vodDownloadLambdaFunctionName),
                "arn:aws:logs:%s:%s:log-group:/aws/lambda/%s:*" % (self.region, self.account, vodDownloadLambdaFunctionName)
            ]
        ))
        # Download Lambda needs to be able to read S3.
        vodDownloadLambdaRole.add_to_policy(iam.PolicyStatement(
            actions=[ "s3:ListBucket" ],
            resources=[ destinationBucketArn ]
        ))
        # Download Lambda needs to be able to write to S3.
        vodDownloadLambdaRole.add_to_policy(iam.PolicyStatement(
            actions=[
                "s3:PutObject",
                "s3:GetObject"
                ],
            resources=[ "%s/*" % destinationBucketArn ]
        ))
        # Download Lambda needs to be able to create a KMS Data Key
        vodDownloadLambdaRole.add_to_policy(iam.PolicyStatement(
            actions=["kms:GenerateDataKey"],
            resources=[
                "arn:aws:kms:%s:%s:key/%s" % (self.region, self.account, masterKmsKey.key_id)
            ]
        ))
        # Download Lambda needs to be able to pull content from secure MediaPackage V2 endpoints
        # Note: This policy will entitle the vod migrator to download content from any MediaPacakge V2
        # channel in the account.
        vodDownloadLambdaRole.add_to_policy(iam.PolicyStatement(
            actions=["mediapackagev2:GetObject"],
            resources=[
                "arn:aws:mediapackagev2:%s:%s:channelGroup/*" % (self.region, self.account)
            ]
        ))

        return vodDownloadLambdaRole
    

    def createStepFunctionRole(self, notificationTopic, vodDownloadLambda, destinationBucketArn, masterKmsKey, logDestination):
        stepFunctionRole = iam.Role(self, "StateMachineRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com")
        )

        stepFunctionPolicy = iam.Policy(self, "StateMachinePolicy",
            statements=[
                # Allow read access to all S3 buckets
                iam.PolicyStatement(
                    actions=["s3:ListBucket"],
                    resources=[ destinationBucketArn ]
                ),
                # Allow State Machine to publish to SNS Topic
                iam.PolicyStatement(
                    actions=["sns:Publish"],
                    resources=[
                        notificationTopic.topic_arn
                    ]
                ),
                # Allow State Machine to invoke Lambda Function
                # Required to execute VOD download function
                iam.PolicyStatement(
                    actions=["lambda:InvokeFunction"],
                    resources=[
                        vodDownloadLambda.function_arn
                    ]
                ),
                # Allow state machine to create a KMS Data Key
                iam.PolicyStatement(
                    actions=[
                        "kms:GenerateDataKey",
                        "kms:Decrypt"
                    ],
                    resources=[
                        "arn:aws:kms:%s:%s:key/%s" % (self.region, self.account, masterKmsKey.key_id)
                    ]
                ),
                # Commented out below permissions as logging on step functions only works if permissions are
                # given to all resources (i.e. '*'). Such permissions are too permissive for a sample
                # project. When this issue is resolve code can be uncommented.
                # # Allow state machine to log to CloudWatch
                # iam.PolicyStatement(
                #     actions=[
                #         "logs:CreateLogDelivery",
                #         "logs:CreateLogStream",
                #         "logs:GetLogDelivery",
                #         "logs:UpdateLogDelivery",
                #         "logs:DeleteLogDelivery",
                #         "logs:ListLogDeliveries",
                #         "logs:PutLogEvents",
                #         "logs:PutResourcePolicy",
                #         "logs:DescribeResourcePolicies",
                #         "logs:DescribeLogGroups"
                #     ],
                #     resources=[
                #         logDestination,
                #         "%s:*" % logDestination
                #     ]
                # )
            ]
        )

        stepFunctionRole.attach_inline_policy(stepFunctionPolicy)

        return ( stepFunctionRole, stepFunctionPolicy )


def generateRandomString(length):
    character_set = string.ascii_letters + string.digits
    return ''.join(secrets.choice(character_set) for i in range(length))
