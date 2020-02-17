package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.services.forecast.AmazonForecast;
import com.amazonaws.services.forecast.model.CreatePredictorRequest;
import com.amazonaws.services.forecast.model.DescribePredictorRequest;
import com.amazonaws.services.forecast.model.FeaturizationConfig;
import com.amazonaws.services.forecast.model.InputDataConfig;
import com.amazonaws.services.forecast.model.ResourceNotFoundException;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_GROUP_ARN_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATA_FREQUENCY_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATA_FREQUENCY_SECONDS_MAPPING;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.PREDICTOR_ARN_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.PREDICTOR_NAME_KEY;

@Slf4j
public class CreatePredictorHandler extends AbstractPredictionGenerationLambdaHandler {

    private static final String PREDICTOR_RESOURCE_TYPE = "predictor";
    private static final String FORECAST_PREDICTOR_ALGORITHM_ARN;
    static {
        String forecastPredictorAlgorithmArn = System.getenv("FORECAST_PREDICTOR_ALGORITHM_ARN");
        log.info(String.format("forecastPredictorAlgorithmArn getting from environment variable is [%s]", forecastPredictorAlgorithmArn));
        FORECAST_PREDICTOR_ALGORITHM_ARN = forecastPredictorAlgorithmArn;
    }

    @VisibleForTesting
    static final int SECONDS_IN_A_DAY = 86400;

    public CreatePredictorHandler() {
        super();
    }

    public CreatePredictorHandler(final AmazonForecast forecastClient) {
        super(forecastClient);
    }

    @Override
    public void process(final Map<String, String> resourceIdMap) {

        String datasetGroupArn = resourceIdMap.get(DATASET_GROUP_ARN_KEY);
        String predictorName = resourceIdMap.get(PREDICTOR_NAME_KEY);
        String predictorArn = resourceIdMap.get(PREDICTOR_ARN_KEY);
        String dataFrequency = resourceIdMap.get(DATA_FREQUENCY_KEY);
        log.info(String.format(
                "The datasetGroupArn, %s, and forecastFrequency getting from resourceIdMap are [%s], [%s], and [%s]",
                PREDICTOR_RESOURCE_TYPE, datasetGroupArn, predictorName, dataFrequency));


        // Check if predictor exists
        try {
            String currentStatus = describePredictorStatus(predictorArn);
            if (takeActionByResourceStatus(currentStatus, PREDICTOR_RESOURCE_TYPE, predictorArn)) {
                return;
            }
        } catch (ResourceNotFoundException e) {
            log.info(String.format("Cannot find %s with arn [%s]. Proceed to create a new one",
                    PREDICTOR_RESOURCE_TYPE, predictorArn));
        }

        // Create the new predictor
        int forecastHorizonInDays = Integer.parseInt(System.getenv("FORECAST_HORIZON_IN_DAYS"));
        int forecastHorizon = forecastHorizonInDays * SECONDS_IN_A_DAY / DATA_FREQUENCY_SECONDS_MAPPING.get(dataFrequency);
        log.info(String.format("[forecastHorizonInDay:%d]*[SECONDS_IN_A_DAY:%d]/[DATA_FREQUENCY_SECONDS:%d]=[forecastHorizon:%d]",
                forecastHorizonInDays, SECONDS_IN_A_DAY, DATA_FREQUENCY_SECONDS_MAPPING.get(dataFrequency), forecastHorizon));

        createPredictor(forecastHorizon, dataFrequency, datasetGroupArn, predictorName, FORECAST_PREDICTOR_ALGORITHM_ARN);
        log.info("finish triggering CreatePredictorCall.");

        String newStatus = describePredictorStatus(predictorArn);
        takeActionByResourceStatus(newStatus, PREDICTOR_RESOURCE_TYPE, predictorName);
    }

    private void createPredictor(final int forecastHorizon,
                                 final String forecastFrequency,
                                 final String datasetGroupArn,
                                 final String predictorName,
                                 final String predictorAlgorithmArn) {

        CreatePredictorRequest createPredictorRequest = new CreatePredictorRequest()
                .withForecastHorizon(forecastHorizon)
                .withFeaturizationConfig(new FeaturizationConfig().withForecastFrequency(forecastFrequency))
                .withInputDataConfig(new InputDataConfig().withDatasetGroupArn(datasetGroupArn))
                .withPredictorName(predictorName);
        if (StringUtils.isBlank(predictorAlgorithmArn)) {
            createPredictorRequest.setPerformAutoML(true);
        } else {
            createPredictorRequest.setAlgorithmArn(predictorAlgorithmArn);
        }

        forecastClient.createPredictor(createPredictorRequest);
    }

    private String describePredictorStatus(final String predictorArn) {
        DescribePredictorRequest describePredictorRequest = new DescribePredictorRequest();
        describePredictorRequest.setPredictorArn(predictorArn);
        return forecastClient.describePredictor(describePredictorRequest).getStatus();
    }
}
