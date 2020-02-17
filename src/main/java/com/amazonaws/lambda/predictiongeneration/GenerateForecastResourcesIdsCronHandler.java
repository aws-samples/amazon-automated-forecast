package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.dagger.DaggerLambdaFunctionsComponent;
import com.amazonaws.services.forecast.AmazonForecast;
import com.amazonaws.services.forecast.model.DatasetSummary;
import com.amazonaws.services.forecast.model.PredictorSummary;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_ARN_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_IMPORT_JOB_ARN_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_IMPORT_JOB_NAME_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_IMPORT_JOB_NAME_PREFIX;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.FORECAST_ARN_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.FORECAST_EXPORT_JOB_ARN_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.FORECAST_EXPORT_JOB_NAME_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.FORECAST_EXPORT_JOB_NAME_PREFIX;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.FORECAST_NAME_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.FORECAST_NAME_PREFIX;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.PREDICTOR_ARN_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.deriveForecastResourceArnPrefixFromLambdaFunctionArn;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.getLatestDataset;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.getLatestPredictor;

@Slf4j
public class GenerateForecastResourcesIdsCronHandler implements RequestHandler<Void, String> {

    private final Clock clock;

    @Inject
    @NonNull
    AmazonForecast forecastClient;

    public GenerateForecastResourcesIdsCronHandler() {
        this(Clock.systemUTC());
    }

    public GenerateForecastResourcesIdsCronHandler(final Clock clock) {
        this.clock = clock;
        DaggerLambdaFunctionsComponent.create().inject(this);
    }

    public GenerateForecastResourcesIdsCronHandler(final Clock clock,
                                                   final AmazonForecast forecastClient) {
        this.clock = clock;
        this.forecastClient = forecastClient;
    }

    public String handleRequest(Void input, Context context) {

        final DatasetSummary latestDataset = getLatestDataset(forecastClient);
        if (latestDataset == null) {
            throw new IllegalStateException("cannot find any dataset");
        }

        final PredictorSummary latestPredictor = getLatestPredictor(forecastClient);
        if (latestPredictor == null) {
            throw new IllegalStateException("cannot find any predictor");
        }

        String functionArn = context.getInvokedFunctionArn();
        String forecastResourceArnPrefix = deriveForecastResourceArnPrefixFromLambdaFunctionArn(functionArn);

        long currentTime = clock.millis();
        Map<String,String> cronResourceIdMap = buildCronResourceIdMap(currentTime,
                forecastResourceArnPrefix, latestDataset.getDatasetName(), latestPredictor.getPredictorArn());

        String cronResourceIdMapAsJson;
        try {
            cronResourceIdMapAsJson = new ObjectMapper().writeValueAsString(cronResourceIdMap);
        } catch (JsonProcessingException e) {
            String errorMsg = e.getMessage();
            log.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }

        log.info("Returning cronResourceIdMapAsJson value is " + cronResourceIdMapAsJson);
        return cronResourceIdMapAsJson;
    }

    @VisibleForTesting
    static Map<String, String> buildCronResourceIdMap(final long timestamp,
                                                      final String forecastResourceArnPrefix,
                                                      final String datasetName,
                                                      final String predictorArn) {

        String datasetImportJobName = DATASET_IMPORT_JOB_NAME_PREFIX + timestamp;
        String forecastName = FORECAST_NAME_PREFIX + timestamp;
        String forecastExportJobName = FORECAST_EXPORT_JOB_NAME_PREFIX + timestamp;

        return ImmutableMap.<String, String>builder()
                .put(DATASET_ARN_KEY, forecastResourceArnPrefix + "dataset/" + datasetName)
                .put(DATASET_IMPORT_JOB_NAME_KEY, datasetImportJobName)
                .put(DATASET_IMPORT_JOB_ARN_KEY, forecastResourceArnPrefix
                        + "dataset-import-job/" + datasetName + "/" + datasetImportJobName)
                .put(PREDICTOR_ARN_KEY, predictorArn)
                .put(FORECAST_NAME_KEY, forecastName)
                .put(FORECAST_ARN_KEY, forecastResourceArnPrefix + "forecast/" + forecastName)
                .put(FORECAST_EXPORT_JOB_NAME_KEY, forecastExportJobName)
                .put(FORECAST_EXPORT_JOB_ARN_KEY, forecastResourceArnPrefix
                        + "forecast-export-job/" + forecastName + "/" + forecastExportJobName)
                .build();
    }
}
