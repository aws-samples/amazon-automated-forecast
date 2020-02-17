package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.services.forecast.AmazonForecast;
import com.amazonaws.services.forecast.model.CreateDatasetImportJobRequest;
import com.amazonaws.services.forecast.model.DataSource;
import com.amazonaws.services.forecast.model.DescribeDatasetImportJobRequest;
import com.amazonaws.services.forecast.model.ResourceNotFoundException;
import com.amazonaws.services.forecast.model.S3Config;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_ARN_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_IMPORT_JOB_ARN_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_IMPORT_JOB_NAME_KEY;

@Slf4j
public class CreateDatasetImportJobHandler extends AbstractPredictionGenerationLambdaHandler {

    private static final String FORECAST_IMPORT_TRAINING_DATA_ROLE_ARN;
    static {
        String forecastImportTrainingDataRoleArn = System.getenv("FORECAST_IMPORT_TRAINING_DATA_ROLE_ARN");
        log.info(String.format("forecastImportTrainingDataRoleArn getting from environment variable is [%s]",
                forecastImportTrainingDataRoleArn));
        FORECAST_IMPORT_TRAINING_DATA_ROLE_ARN = forecastImportTrainingDataRoleArn;
    }
    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String FORECAST_TRAINING_DATA_S3_URI;
    static {
        String s3TrainingDataBucket = System.getenv("PREDICTION_S3_BUCKET_NAME");
        String s3TrainingDataFolder = System.getenv("SRC_S3_FOLDER");
        String s3TrainingDataFileName = System.getenv("S3_TRAINING_DATA_FILE_NAME");
        String forecastTrainingDataS3Uri = String.format("s3://%s/%s/%s",
                s3TrainingDataBucket,
                s3TrainingDataFolder,
                s3TrainingDataFileName);
        log.info(String.format("The forecastTrainingDataS3Uri getting from env variables is %s",
                forecastTrainingDataS3Uri));
        FORECAST_TRAINING_DATA_S3_URI = forecastTrainingDataS3Uri;
    }
    private static final String DATASET_IMPORT_JOB_RESOURCE_TYPE = "datasetImportJob";

    public CreateDatasetImportJobHandler() {
        super();
    }

    public CreateDatasetImportJobHandler(final AmazonForecast forecastClient) {
        super(forecastClient);
    }

    @Override
    public void process(final Map<String, String> resourceIdMap) {
        String datasetArn = resourceIdMap.get(DATASET_ARN_KEY);
        String datasetImportJobName = resourceIdMap.get(DATASET_IMPORT_JOB_NAME_KEY);
        String datasetImportJobArn = resourceIdMap.get(DATASET_IMPORT_JOB_ARN_KEY);
        log.info(String.format(
                "The datasetArn, datasetImportJobName, and datasetImportJobArn getting from resourceIdMap are [%s], [%s], and [%s]",
                datasetArn, datasetImportJobName, datasetImportJobArn));

        // Check if dataset import job exists
        try {
            String currentStatus = describeDatasetImportJobStatus(datasetImportJobArn);
            if (takeActionByResourceStatus(currentStatus, DATASET_IMPORT_JOB_RESOURCE_TYPE, datasetImportJobArn)) {
                return;
            }
        } catch (ResourceNotFoundException e) {
            log.info(String.format("Cannot find %s, %s. Proceed to create a new one",
                    DATASET_IMPORT_JOB_RESOURCE_TYPE, datasetImportJobArn));
        }

        // Create the dataset import job if found no import job for given dataset name
        createDatasetImportJob(datasetImportJobName,
                datasetArn,
                FORECAST_TRAINING_DATA_S3_URI,
                FORECAST_IMPORT_TRAINING_DATA_ROLE_ARN,
                TIMESTAMP_FORMAT);
        log.info("finish triggering CreateDatasetImportJobCall.");

        String newStatus = describeDatasetImportJobStatus(datasetImportJobArn);
        takeActionByResourceStatus(newStatus, DATASET_IMPORT_JOB_RESOURCE_TYPE, datasetImportJobArn);
    }

    private void createDatasetImportJob(final String datasetImportJobName,
                                        final String datasetArn,
                                        final String s3Uri,
                                        final String roleArn,
                                        final String timestampFormat) {
        CreateDatasetImportJobRequest createDatasetImportJobRequest = new CreateDatasetImportJobRequest();
        createDatasetImportJobRequest.setDatasetImportJobName(datasetImportJobName);
        createDatasetImportJobRequest.setDatasetArn(datasetArn);
        createDatasetImportJobRequest.setDataSource(
                new DataSource().withS3Config(
                        new S3Config().withPath(s3Uri).withRoleArn(roleArn))
        );
        createDatasetImportJobRequest.setTimestampFormat(timestampFormat);
        forecastClient.createDatasetImportJob(createDatasetImportJobRequest);
    }

    private String describeDatasetImportJobStatus(final String dataseImportJobArn) {
        DescribeDatasetImportJobRequest describeDatasetImportJobRequest = new DescribeDatasetImportJobRequest();
        describeDatasetImportJobRequest.setDatasetImportJobArn(dataseImportJobArn);
        return forecastClient.describeDatasetImportJob(describeDatasetImportJobRequest).getStatus();
    }
}
