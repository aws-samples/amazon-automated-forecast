package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.services.forecast.AmazonForecast;
import com.amazonaws.services.forecast.model.CreateForecastExportJobRequest;
import com.amazonaws.services.forecast.model.DataDestination;
import com.amazonaws.services.forecast.model.DescribeForecastExportJobRequest;
import com.amazonaws.services.forecast.model.ResourceNotFoundException;
import com.amazonaws.services.forecast.model.S3Config;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.FORECAST_ARN_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.FORECAST_EXPORT_JOB_ARN_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.FORECAST_EXPORT_JOB_NAME_KEY;

@Slf4j
public class CreateForecastExportJobHandler extends AbstractPredictionGenerationLambdaHandler {

    private static final String FORECAST_EXPORT_JOB_RESOURCE_TYPE = "forecastExportJob";
    private static final String FORECAST_EXPORT_RESULT_ROLE_ARN;
    static {
        String forecastExportResultRoleArn = System.getenv("FORECAST_EXPORT_RESULT_ROLE_ARN");
        log.info(String.format("forecastExportResultRoleArn getting from environment variable is [%s]", forecastExportResultRoleArn));
        FORECAST_EXPORT_RESULT_ROLE_ARN = forecastExportResultRoleArn;
    }
    private static final String FORECAST_EXPORT_RESULT_S3_URI;
    static {
        String s3ExportResultBucket = System.getenv("PREDICTION_S3_BUCKET_NAME");
        String s3ExportResultFolder = System.getenv("TGT_S3_FOLDER");
        String forecastExportResultS3Uri = String.format("s3://%s/%s", s3ExportResultBucket, s3ExportResultFolder);
        log.info(String.format("The forecastExportResultS3Uri getting from env variables is %s",
                forecastExportResultS3Uri));
        FORECAST_EXPORT_RESULT_S3_URI = forecastExportResultS3Uri;
    }

    public CreateForecastExportJobHandler() {
        super();
    }

    public CreateForecastExportJobHandler(AmazonForecast forecastClient) {
        super(forecastClient);
    }

    @Override
    public void process(final Map<String, String> resourceIdMap) {

        String forecastExportJobName = resourceIdMap.get(FORECAST_EXPORT_JOB_NAME_KEY);
        String forecastExportJobArn = resourceIdMap.get(FORECAST_EXPORT_JOB_ARN_KEY);
        String forecastArn = resourceIdMap.get(FORECAST_ARN_KEY);
        log.info(String.format(
                "The forecastExportJobName, forecastExportJobArn, and forecastArn getting from resourceIdMap are [%s], [%s], and [%s]",
                forecastExportJobName, forecastExportJobArn, forecastArn));

        // Check if forecastExportJob exists
        try {
            String currentStatus = describeForecastExportJobStatus(forecastExportJobArn);
            if (takeActionByResourceStatus(currentStatus, FORECAST_EXPORT_JOB_RESOURCE_TYPE, forecastExportJobArn)) {
                return;
            }
        } catch (ResourceNotFoundException e) {
            log.info(String.format("Cannot find %s with arn [%s]. Proceed to create a new one",
                    FORECAST_EXPORT_JOB_RESOURCE_TYPE, forecastExportJobArn));
        }

        // create a new forecastExportJob
        createForecastExportJob(forecastExportJobName, forecastArn, FORECAST_EXPORT_RESULT_ROLE_ARN, FORECAST_EXPORT_RESULT_S3_URI);
        log.info("finish triggering CreateForecastExportJobCall.");

        String newStatus = describeForecastExportJobStatus(forecastExportJobArn);
        takeActionByResourceStatus(newStatus, FORECAST_EXPORT_JOB_RESOURCE_TYPE, forecastExportJobArn);
    }

    private String describeForecastExportJobStatus(final String forecastExportJobArn) {
        DescribeForecastExportJobRequest describeForecastExportJobRequest = new DescribeForecastExportJobRequest();
        describeForecastExportJobRequest.setForecastExportJobArn(forecastExportJobArn);
        return forecastClient.describeForecastExportJob(describeForecastExportJobRequest).getStatus();
    }

    private void createForecastExportJob(final String forecastExportJobName,
                                         final String forecastArn,
                                         final String roleArn,
                                         final String s3Uri) {
        CreateForecastExportJobRequest createForecastExportJobRequest = new CreateForecastExportJobRequest();
        createForecastExportJobRequest.setForecastExportJobName(forecastExportJobName);
        createForecastExportJobRequest.setDestination(
                new DataDestination().withS3Config(
                        new S3Config().withPath(s3Uri).withRoleArn(roleArn)
                )
        );
        createForecastExportJobRequest.setForecastArn(forecastArn);

        forecastClient.createForecastExportJob(createForecastExportJobRequest);
    }
}
