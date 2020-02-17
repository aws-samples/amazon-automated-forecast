package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.services.forecast.AmazonForecast;
import com.amazonaws.services.forecast.model.CreateForecastRequest;
import com.amazonaws.services.forecast.model.DescribeForecastRequest;
import com.amazonaws.services.forecast.model.ResourceNotFoundException;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.FORECAST_ARN_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.FORECAST_NAME_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.PREDICTOR_ARN_KEY;

@Slf4j
public class CreateForecastHandler extends AbstractPredictionGenerationLambdaHandler {

    private static final String FORECAST_RESOURCE_TYPE = "forecast";

    public CreateForecastHandler() {
        super();
    }

    public CreateForecastHandler(final AmazonForecast forecastClient) {
        super(forecastClient);
    }

    @Override
    public void process(final Map<String, String> resourceIdMap) {

        String forecastName = resourceIdMap.get(FORECAST_NAME_KEY);
        String forecastArn = resourceIdMap.get(FORECAST_ARN_KEY);
        String predictorArn = resourceIdMap.get(PREDICTOR_ARN_KEY);
        log.info(String.format(
                "The forecastName, forecastArn, and predictorArn getting from resourceIdMap are [%s], [%s], and [%s]",
                forecastName, forecastArn, predictorArn));

        // Check if forecast exists
        try {
            String currentStatus = describeForecastStatus(forecastArn);
            if (takeActionByResourceStatus(currentStatus, FORECAST_RESOURCE_TYPE, forecastArn)) {
                return;
            }
        } catch (ResourceNotFoundException e) {
            log.info(String.format("Cannot find %s with arn [%s]. Proceed to create a new one",
                    FORECAST_RESOURCE_TYPE, forecastArn));
        }

        // create a new forecast
        createForecast(forecastName, predictorArn);
        log.info("finish triggering CreateForecastCall.");

        String newStatus = describeForecastStatus(forecastArn);
        takeActionByResourceStatus(newStatus, FORECAST_RESOURCE_TYPE, forecastArn);
    }

    private void createForecast(final String forecastName,
                                final String predictorArn) {
        CreateForecastRequest createForecastRequest = new CreateForecastRequest();
        createForecastRequest.setForecastName(forecastName);
        createForecastRequest.setPredictorArn(predictorArn);

        forecastClient.createForecast(createForecastRequest);
    }

    private String describeForecastStatus(final String forecastArn) {
        DescribeForecastRequest describeForecastRequest = new DescribeForecastRequest();
        describeForecastRequest.setForecastArn(forecastArn);

        return forecastClient.describeForecast(describeForecastRequest).getStatus();
    }
}
