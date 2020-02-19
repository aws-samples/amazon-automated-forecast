# SAM Application for Automated Forecast

This is a sample application to demonstrate how to build a system around
[the time-series forecasting service Amazon Forecast](https://aws.amazon.com/forecast/),
which can automatically:
* publish the historical demand to S3 bucket as the training data,
* create the machine learning model and generate the prediction result,
* load the latest prediction result to DynamoDB for querying.

Details can be found in the [blog post](https://aws.amazon.com/blogs/machine-learning/automating-your-amazon-forecast-workflow-with-lambda-step-functions-and-cloudwatch-events-rule/)

```bash
.
├── README.md                   <-- This instructions file
├── LICENSE.txt                 <-- MIT No Attribution License (MIT-0)
├── NOTICE.txt                  <-- Copyright notices
├── build.gradle                <-- Java dependencies
├── gradlew
├── gradlew.bat
├── src
│   ├── main
│   │   └── resources                       <-- Contains a dummy demand records csv file used for simulating the database                     
│   │   └── java
│   │       ├── com.amazonaws.dagger            <-- Classes to manage Dagger 2 dependency injection
│   │       │   ├── AWSClientModule.java            <-- Provides dependencies like the Forecast client for injection
│   │       │   └── LambdaFunctionsComponent.java   <-- Contains inject methods for handler entrypoints
│   │       └── com.amazonaws.lambda            <-- Source code for lambda functions
│   │           ├── demandpublishing                <-- Lambda functions for demand publishing component
|   |           |   ├── DemandRecord.java                               <-- POJO shape for parsing the demand record from CSV file 
|   |           |   ├── PublishDemandHandler.java                       <-- Lambda functions for querying the historical demand and publish it to S3
│   │           ├── predictiongeneration            <-- Lambda functions for prediction generation component
|   |           |   ├── exception                                       <-- Source code for custom exceptions
|   |           |   |   ├── ResourceCleanupInProgressException.java         <-- Can be thrown when the resource cannot be immediately deleted
|   |           |   |   ├── ResourceSetupFailureException.java              <-- Can be thrown when the resource failed to create
|   |           |   |   └── ResourceSetupInProgressException.java           <-- Can be thrown when the resource cannot be immediately created
|   |           |   ├── PredictionGenerationUtils.java                  <-- Contains common util methods
|   |           |   ├── GenerateForecastResourcesIdsHandler.java        <-- Generate required forecast resource ids for model generation
|   |           |   ├── GenerateForecastResourcesIdsCronHandler.java    <-- Generate required forecast resource ids for forecast generation
|   |           |   ├── AbstractPredictionGenerationLambdaHandler.java  <-- Abstract hanlder contains methods can be shared by inherited handlers
|   |           |   ├── CreateDatasetHandler.java                       <-- Function implementation for creating forecast dataset resource
|   |           |   ├── CreateDatasetGroupHandler.java                  <-- Function implementation for creating forecast dataset group resource
|   |           |   ├── CreateDatasetImportJobHandler.java              <-- Function implementation for creating forecast dataset import job resource
|   |           |   ├── CreatePredictorHandler.java                     <-- Function implementation for creating forecast predictor (ML model) resource
|   |           |   ├── CreateForecastHandler.java                      <-- Function implementation for creating forecast resource
|   |           |   ├── CreateForecastExportJobHandler.java             <-- Function implementation for creating forecast export job resource
|   |           |   ├── DeleteOutdatedForecastExportJobsHandler.java    <-- Function implementation for deleting expired export job resources
|   |           |   ├── DeleteOutdatedForecastsHandler.java             <-- Function implementation for deleting expired forecast resources
|   |           |   ├── DeleteOutdatedPredictorsHandler.java            <-- Function implementation for deleting expired predictor resources
|   |           |   ├── DeleteOutdatedDatasetImportJobsHandler.java     <-- Function implementation for deleting expired dataset import job resources
|   |           |   ├── DeleteOutdatedDatasetsHandler.java              <-- Function implementation for deleting expired dataset resources
|   |           |   └── DeleteOutdatedDatasetGroupsHandler.java         <-- Function implementation for deleting expired dataset group resources
│   │           └── queryingpredictionresult        <-- Lambda functions for querying prediction result component
|   |               ├── LoadDataFromS3ToDynamoDBHandler.java            <-- Function implementation for loading data from S3 to DynamoDB table
|   |               └── PredictionResultItem.java                       <-- POJO shape for a prediction result record
│   └── test                                <-- Unit tests
│       └── resources                           <-- Contains dummy prediction result csv file used for testing LoadDataFromS3ToDynamoDBHandler.java
│       └── java
│           └── com.amazonaws.lambda                <-- Unit tests for handlers
│               ├── demandpublishing                    <-- Unit tests for demand publishing related handlers
│               |   ├── PublishDemandHandlerTest.java       <-- Unit tests for PublishDemandHandler.java  
│               ├── predictiongeneration                <-- Unit tests for prediction generation related handlers
│               |   ├── GenerateForecastResourcesIdsHandlerTest.java        <-- Unit tests for GenerateForecastResourcesIdsHandler.java  
│               |   ├── GenerateForecastResourcesIdsCronHandlerTest.java    <-- Unit tests for GenerateForecastResourcesIdsCronHandler.java  
│               |   ├── CreateDatasetHandlerTest.java                       <-- Unit tests for CreateDatasetHandler.java  
│               |   ├── CreateDatasetGroupHandlerTest.java                  <-- Unit tests for CreateDatasetGroupHandler.java  
│               |   ├── CreatePredictorHandlerTest.java                     <-- Unit tests for CreatePredictorHandler.java  
│               |   ├── CreateForecastHandlerTest.java                      <-- Unit tests for CreateForecastHandler.java  
│               |   ├── CreateForecastExportJobHandlerTest.java             <-- Unit tests for CreateForecastExportJobHandler.java  
│               |   ├── DeleteOutdatedForecastExportJobsHandlerTest.java    <-- Unit tests for DeleteOutdatedForecastExportJobsHandler.java  
│               |   ├── DeleteOutdatedForecastsHandlerTest.java             <-- Unit tests for DeleteOutdatedForecastsHandler.java  
│               |   ├── DeleteOutdatedPredictorsHandlerTest.java            <-- Unit tests for DeleteOutdatedPredictorsHandler.java  
│               |   ├── DeleteOutdatedDatasetImportJobsHandlerTest.java     <-- Unit tests for DeleteOutdatedDatasetImportJobsHandler.java  
│               |   ├── DeleteOutdatedDatasetImportJobsHandlerTest.java     <-- Unit tests for DeleteOutdatedDatasetImportJobsHandler.java  
│               |   ├── DeleteOutdatedDatasetsHandlerTest.java              <-- Unit tests for DeleteOutdatedDatasetsHandler.java  
│               |   └── DeleteOutdatedDatasetGroupsHandlerTest.java         <-- Unit tests for DeleteOutdatedDatasetGroupsHandler.java  
│               └── queryingpredictionresult            <-- Unit tests for querying prediction result related handlers
│                   └── LoadDataFromS3ToDynamoDBHandlerTest.java            <-- Unit tests for LoadDataFromS3ToDynamoDBHandler.java  
└── template.yaml               <-- Contains cloudformation resources for lambda, S3, step function, cloudwatch event, dynamodb, iam role, etc.
```

## Requirements

* AWS CLI already configured with at least PowerUser permission
* [Gradle Build Tool](https://gradle.org/)
* [Java SE Development Kit 8 installed](https://www.oracle.com/java/technologies/javase-jdk8-downloads.html)
* [SAM CLI](https://github.com/awslabs/aws-sam-cli)

## Setup process

### Installing dependencies

We use `gradle` to install our dependencies and package our application into a JAR file:

```bash
gradle build
```

## Packaging and deployment

AWS Lambda Java runtime accepts either a zip file or a standalone JAR file - We use the latter in
this example. SAM will use `CodeUri` property to know where to look up for both application and
dependencies. As all functions use the same jar, we declare it in the Globals:

```yaml
...
    Globals:
      Function:
        AutoPublishAlias: live
        DeploymentPreference:
          Type: AllAtOnce
        MemorySize: 1024
        Runtime: java8
        Timeout: 180
        ReservedConcurrentExecutions: 2 # There can be two state machines executing the same function at the same time
        CodeUri: .
```

Firstly, we need a `S3 bucket` where we can upload our Lambda functions packaged as ZIP before we
deploy anything - If you don't have a S3 bucket to store code artifacts then this is a good time to
create one:

```bash
export BUCKET_NAME=my_cool_new_bucket
aws s3 mb s3://$BUCKET_NAME
```

Next, run the following command to package our Lambda function to S3:

```bash
sam package \
    --template-file template.yaml \
    --output-template-file packaged.yaml \
    --s3-bucket $BUCKET_NAME
```

Next, the following command will create a Cloudformation Stack and deploy your SAM resources.

```bash
sam deploy \
    --template-file packaged.yaml \
    --stack-name sam-orderHandler \
    --capabilities CAPABILITY_IAM
```

> **See [Serverless Application Model (SAM) HOWTO Guide](https://github.com/awslabs/serverless-application-model/blob/master/HOWTO.md) for more details in how to get started.**

## Testing

### Running unit tests
We use `JUnit` for testing our code. You can run unit tests with the following command:

```bash
gradle test
```
