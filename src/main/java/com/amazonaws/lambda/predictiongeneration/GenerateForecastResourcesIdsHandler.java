package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.dagger.DaggerLambdaFunctionsComponent;
import com.amazonaws.lambda.predictiongeneration.exception.ResourceSetupInProgressException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;

import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_ARN_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_GROUP_ARN_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_GROUP_NAME_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_GROUP_NAME_PREFIX;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_IMPORT_JOB_ARN_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_IMPORT_JOB_NAME_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_IMPORT_JOB_NAME_PREFIX;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_NAME_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_NAME_PREFIX;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATA_FREQUENCY_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.FORECAST_ARN_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.FORECAST_NAME_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.FORECAST_NAME_PREFIX;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.FORECAST_RESOURCE_ARN_PREFIX_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.PREDICTOR_ARN_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.PREDICTOR_NAME_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.PREDICTOR_NAME_PREFIX;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.deriveForecastResourceArnPrefixFromLambdaFunctionArn;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.getForecastDataFrequencyStr;

@Slf4j
public class GenerateForecastResourcesIdsHandler implements RequestHandler<Void, String> {

    private static final Duration PREDICTION_WINDOW_SIZE_DURATION = Duration.ofSeconds(2000);
    private static final String PREDICTION_S3_BUCKET_NAME = System.getenv("PREDICTION_S3_BUCKET_NAME");
    private static final String PREDICTION_S3_HISTORICAL_DEMAND_FOLDER = System.getenv("SRC_S3_FOLDER");
    private static final String PREDICTION_S3_HISTORICAL_DEMAND_FILE_NAME = System.getenv("S3_TRAINING_DATA_FILE_NAME");

    private final Clock clock;

    @Inject
    @NonNull
    AmazonS3 s3Client;

    public GenerateForecastResourcesIdsHandler() {
        this(Clock.systemUTC());
    }

    public GenerateForecastResourcesIdsHandler(final Clock clock) {
        this.clock = clock;
        DaggerLambdaFunctionsComponent.create().inject(this);
    }

    @VisibleForTesting
    GenerateForecastResourcesIdsHandler(final Clock clock, final AmazonS3 s3Client) {
        this.clock = clock;
        this.s3Client = s3Client;
    }

    public String handleRequest(Void input, Context context) {

        sanityCheck();

        long currentTime = clock.millis();

        String functionArn = context.getInvokedFunctionArn();
        String forecastResourceArnPrefix = deriveForecastResourceArnPrefixFromLambdaFunctionArn(functionArn);

        String dataFrequencyValue = getForecastDataFrequencyStr(PREDICTION_WINDOW_SIZE_DURATION);
        Map<String,String> resourceIdMap = buildResourceIdMap(currentTime, forecastResourceArnPrefix, dataFrequencyValue);

        String resourceIdMapAsJson;
        try {
            resourceIdMapAsJson = new ObjectMapper().writeValueAsString(resourceIdMap);
        } catch (JsonProcessingException e) {
            String errorMsg = e.getMessage();
            log.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }

        log.info("Returning resourceIdMapAsJson value is " + resourceIdMapAsJson);
        return resourceIdMapAsJson;
    }

    private void sanityCheck() {
        ObjectMetadata s3ObjectMetadata;
        try {
            GetObjectMetadataRequest getObjectMetadataRequest =
                    new GetObjectMetadataRequest(PREDICTION_S3_BUCKET_NAME,
                            String.format("%s/%s", PREDICTION_S3_HISTORICAL_DEMAND_FOLDER,
                                    PREDICTION_S3_HISTORICAL_DEMAND_FILE_NAME));
            s3ObjectMetadata = s3Client.getObjectMetadata(getObjectMetadataRequest);
        } catch (AmazonS3Exception e) {
            throw new ResourceSetupInProgressException(String.format("Got exception while getting info of the demand source file: %s",
                    e.getMessage()));
        }

        if (s3ObjectMetadata.getContentLength() == 0) {
            throw new ResourceSetupInProgressException("The demand source file is empty");
        }
    }

    @VisibleForTesting
    static Map<String, String> buildResourceIdMap(final long timestamp,
                                                  final String forecastResourceArnPrefix,
                                                  final String dataFrequencyValue) {

        String datasetName = DATASET_NAME_PREFIX + timestamp;
        String datasetGroupName = DATASET_GROUP_NAME_PREFIX + timestamp;
        String datasetImportJobName = DATASET_IMPORT_JOB_NAME_PREFIX + timestamp;
        String predictorName = PREDICTOR_NAME_PREFIX + timestamp;
        String forecastName = FORECAST_NAME_PREFIX + timestamp;

        return ImmutableMap.<String, String>builder()
                .put(FORECAST_RESOURCE_ARN_PREFIX_KEY, forecastResourceArnPrefix)
                .put(DATASET_NAME_KEY, datasetName)
                .put(DATASET_ARN_KEY, forecastResourceArnPrefix + "dataset/" + datasetName)
                .put(DATASET_GROUP_NAME_KEY, datasetGroupName)
                .put(DATASET_GROUP_ARN_KEY, forecastResourceArnPrefix + "dataset-group/" + datasetGroupName)
                .put(DATASET_IMPORT_JOB_NAME_KEY, datasetImportJobName)
                .put(DATASET_IMPORT_JOB_ARN_KEY, forecastResourceArnPrefix
                        + "dataset-import-job/" + datasetName + "/" + datasetImportJobName)
                .put(PREDICTOR_NAME_KEY, predictorName)
                .put(PREDICTOR_ARN_KEY, forecastResourceArnPrefix + "predictor/" + predictorName)
                .put(FORECAST_NAME_KEY, forecastName)
                .put(FORECAST_ARN_KEY, forecastResourceArnPrefix + "forecast/" + forecastName)
                .put(DATA_FREQUENCY_KEY, dataFrequencyValue)
                .build();
    }
}
