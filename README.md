# General Overview

The VOD Migrator workflow simplifies the process of downloading HLS, DASH and CMAF assets from publicly available URLs to S3. This can be a useful workflow for organisations looking to migrate content from legacy video platforms to AWS when the source content is not available or the content is in a format not supported for [AWS Elemental MediaPackage VOD ingest](https://docs.aws.amazon.com/mediapackage/latest/ug/supported-inputs-vod.html).

Once downloaded to S3 the content can be used in a variety of ways. Below are some example use cases:
1. Public Distribution - Create CloudFront Distribution using the S3 bucket as an origin to make content publicly available.
2. Public Distribution with Access Control - Create a CloudFront Distribution configured with the [Secure Media Delivery at the Edge on AWS](https://aws.amazon.com/solutions/implementations/secure-media-delivery-at-the-edge/) solution using the S3 bucket as an origin to limit who can access the content.
3. AWS Elemental MediaPackage VOD Input - Downloaded HLS/TS content can potentially be used as an input to create AWS Elemental MediaPackage VOD assets in a variety of formats and optionally apply digital rights management (DRM) encryption.

The workflow uses a [AWS Step Function](https://aws.amazon.com/step-functions/) State Machine to manage the invocation of an AWS Lambda Function to analyse the source content and download the assets resources to S3. In the case where an individual resource takes longer than the maximum 15 minute lambda function runtime the state machine will restart another lambda to continue the transfer from where the previous function ended.

When the workflow completes successfully or detects an error an SNS notification is sent.

![Workflow Architecture Diagram](/images/ArchitectureDiagram.png)

** This project is intended for education purposes only and not for production usage. **

This sample leverages [AWS Step Function](https://aws.amazon.com/step-functions/), [AWS Lambda](https://aws.amazon.com/lambda/) and [Amazon S3](https://aws.amazon.com/s3), [Amazon EventBridge](https://aws.amazon.com/eventbridge/) and [Amazon SNS](https://aws.amazon.com/sns/) to execute the workflow.

Below is an example of a successful state machine execution as it appears in the AWS Console.

![Step Function State Machine Screenshot](/images/workflow.png)

# Getting Started

⚠️ **IMPORTANT NOTE:** **Deploying this demo application in your AWS account will create and consume AWS resources, which will cost money.**

To get the demo running in your own AWS account, follow these instructions.

1. If you do not have an AWS account, please see [How do I create and activate a new Amazon Web Services account?](https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/)
2. Log into the [AWS console](https://console.aws.amazon.com/) if you are not already. Note: If you are logged in as an IAM user, ensure your account has permissions to create and manage the necessary resources and components for this application.

## Deployment

** NOTE: If using an EC2 for deployment ensure the instance has sufficient resources. During testing issues were observed when performing a deploy on an instance smaller than a t2.small with 2GB RAM **

This reference template deploys the VOD Migrator to the default AWS account.

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the `.venv`
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```
python3 -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies for CDK and the lambda layer.

```
pip install --upgrade pip
pip install -r requirements.txt
pip install -r vod_migrator/layer/requirements.txt  -t vod_migrator/layer/python
```

At this point you can now synthesize the CloudFormation template for this code.

```
cdk synth
```

Once the synthesize command is successful, you can deploy the workflow. Ensure you configure the email parameter with a valid recipient address. SNS alerts will be sent to this address.

Note: 'destinationBucketArn' should just be the bucket ARN and not include any prefix.

```
cdk deploy --parameters email=user@sample.com --parameters destinationBucketArn=arn:aws:s3:::<BUCKET_NAME>
```

The 'email' parameter (required) specifies the destination for SNS topic notifications. During the installation emails will be sent to the specified email address to subscribe to two SNS topics. Click in the link to accept the email.

If the solution is being deployed into the default profile no additional environment variables need to be set. If the solution is being deployed using the non-default profile the AWS CLI environment variables should be used to specify the access key id, secret access key and region. Below is an example of the environment variables which need to be set.
```
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_DEFAULT_REGION=us-west-2
```

### Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

# Usage

## Step Function Execution Format

Below is an example of an execution - replace the `SourceLocation` as the streaming URL to download, `DestinationBucket` as S3 bucket name, `DestinationPath` as prefix (equivalent to sub folder):
```
{
  "downloadAssetRequest": {
    "Id": "MyAssetId",
    "SourceLocation": "https://example.com/asset1/asset1.m3u8",
    "DestinationBucket": "<BUCKET_NAME>",
    "DestinationPath": "vod-downloads"                             
  }
}
```

The name of the Step Function State Machine deployed by the template is listed as an output of the deployed CloudFormation Stack.
This can be retrieved under the 'Outputs' tab in the Stack in the CloudFormation Stack.
It can also be retrieve by executing the following CLI command:

    aws cloudformation describe-stacks --stack-name <STACK-NAME> | grep Output

## Command Line Execution Submission

```
aws stepfunctions start-execution --state-machine-arn arn:aws:states:ap-southeast-2:XXXXXXXXXXXX:stateMachine:StateMachine-jvkp8UEK01Ve \
  --input "{
            \"downloadAssetRequest\": {
              \"Id\": \"MyAssetId\",  
              \"SourceLocation\": \"https://example.com/asset1/asset1.m3u8\",
              \"DestinationBucket\": \"<BUCKET_NAME>\",
              \"DestinationPath\": \"vod-downloads\"
            }
          }"

```


## Console Execution Submission

Executions can also be started via the AWS Console.

To start an execution via the console:
1. Open AWS Console
1. Navigate to the Step Functions service
1. If not already selected, select 'State Machines' from the left hand menu
1. Select deployed State Machine (most likely called 'VodMigratorStateMachine')
1. Select 'Start Execution' in the top right hand corner of the console
1. Enter execution input into the input field and click 'Start Execution'

# Harvesting Video On Demand Assets From MediaPackage V2 Endpoints

The VOD Migrator is capable of harvesting assets from MediaPackage V2 endpoints and storing the content on S3. In many cases MediaPackage V2 endpoints will be secured using a resource-based policy. This is covered in detail in the [Security section of the AWS Elemental MediaPackage V2 documentation](https://docs.aws.amazon.com/mediapackage/latest/userguide/security-iam.html).

As part of the VOD Migrator deployment an IAM role with a name similar to "VodMigratorStack-VodDownloadLambdaRoleABCABCABC-XYZXYZXYZ" is created. This role is assumed by the AWS Lambda function responsible for downloading the assets from the endpoint. This AWS Lambda function is invoked by the Step Function State Machine. When the role is created it is assigned a policy to access all the MediaPackage channels in the account and region where the stack is deployed.

**Note: The AWS Lambda Function using the 'VodMigratorStack-VodDownloadLambdaRoleABCABCABC-XYZXYZXYZ' role has permission to access content from *ALL* MediaPackage V2 endpoints in the region where the lambda is deployed. These permissions may be too permissive for many deployments and should be reviewed.**

To improve the security posture of the deployment it is recommended to either:
1. Modify the IAM role to only allow access to a specific set of MediaPackage Channel Groups or Channel Endpoints; or
2. Include an explicit deny in all MediaPackage Endpoint policies.

For more information see the [Origin endpoint authorization](https://docs.aws.amazon.com/mediapackage/latest/userguide/endpoint-auth.html) section of the MediaPackage V2 documentation.


# Using DownloadVod.py as a command line utility

On occasion users may want to run DownloadVod.py as a command line utility (or even from CloudShell) from their local machine.

The following commands download the vod-migrator repository, create a python virtual environment, install dependencies and runs DownloadVod.py:

```
git clone https://github.com/aws-samples/vod-migrator.git
cd vod_migrator/
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r vod_migrator/layer/requirements.txt
vod_migrator/lambda/DownloadVod.py -i <URL> -b <BUCKET_NAME> -d <PATH>
```
# Cleanup

To remove the project try the following command from the local project folder, or go to AWS Console and delete the stack you originally created from CloudFormation.

`cdk destroy`

# Known Limitations
1. The VOD Migrator does not currently work with DASH content where a BaseUrl is specified in the manifest file.
2. The VOD Migrator does not currently support the harvesting on Multi-period DASH assets.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

