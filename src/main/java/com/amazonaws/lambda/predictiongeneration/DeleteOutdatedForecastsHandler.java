package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.services.forecast.AmazonForecast;
import com.amazonaws.services.forecast.model.DeleteForecastRequest;
import com.amazonaws.services.forecast.model.Filter;
import com.amazonaws.services.forecast.model.ForecastSummary;
import com.amazonaws.services.forecast.model.ListForecastsRequest;
import com.amazonaws.services.forecast.model.ListForecastsResult;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.FORECAST_ARN_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.PREDICTOR_ARN_KEY;

@Slf4j
public class DeleteOutdatedForecastsHandler extends AbstractPredictionGenerationLambdaHandler {

    public DeleteOutdatedForecastsHandler() {
        super();
    }

    public DeleteOutdatedForecastsHandler(AmazonForecast forecastClient) {
        super(forecastClient);
    }

    @Override
    public void process(final Map<String, String> resourceIdMap) {
        String currentForecastArn = resourceIdMap.get(FORECAST_ARN_KEY);
        String preservedPredictorArn = resourceIdMap.get(PREDICTOR_ARN_KEY);
        log.info(String.format("The currentForecastArn and preservedPredictorArn getting from resourceIdMap are [%s], [%s]",
                currentForecastArn, preservedPredictorArn));

        // Get all existing predictors and exclude the preserved one
        List<String> outdatedPredictors = listOutdatedPredictorArns(preservedPredictorArn);
        outdatedPredictors.remove(preservedPredictorArn);

        // Delete all forecasts for all outdated predictors
        if (!outdatedPredictors.isEmpty()) {
            outdatedPredictors.forEach(
                    outdatedPredictorArn -> {
                        log.info(String.format("About to delete forecasts for outdated predictorArn [%s]", outdatedPredictorArn));
                        List<ForecastSummary> outdatedForecasts = listActiveForeacasts(outdatedPredictorArn, null);
                        outdatedForecasts.forEach(outdatedForecast -> {
                            deleteForecast(outdatedForecast.getForecastArn());
                        });
                    }
            );
        }

        // Get all existing forecasts associated with given predictorArn
        List<ForecastSummary> outdatedForecasts = listActiveForeacasts(preservedPredictorArn, "ACTIVE");

        // Remove the current processing forecast from the list
        outdatedForecasts.removeIf(forecast -> currentForecastArn.equals(forecast.getForecastArn()));

        int numberOfOutdatedForecasts = outdatedForecasts.size();
        if (numberOfOutdatedForecasts > 5) {
            outdatedForecasts
                    .stream()
                    .sorted(Comparator.comparing(ForecastSummary::getCreationTime))
                    .limit(numberOfOutdatedForecasts - 5)
                    .forEach(forecast -> deleteForecast(forecast.getForecastArn()));
        } else {
            log.info(String.format("We only have %s outdated forecasts, no need to delete", numberOfOutdatedForecasts));
        }
    }

    private void deleteForecast(final String forecastArn) {
        log.info(String.format("About to delete forecastArn [%s].", forecastArn));

        forecastClient.deleteForecast(new DeleteForecastRequest().withForecastArn(forecastArn));
    }

    /**
     * @param predictorArn the predictor arn associated with forecasts
     * @return existing forecasts with Active status
     */
    private List<ForecastSummary> listActiveForeacasts(final String predictorArn,
                                                       final String status) {
        List<ForecastSummary> existingForecasts = new ArrayList<>();
        String nextToken = null;
        do {
            ListForecastsRequest listForecastsRequest = new ListForecastsRequest();
            List<Filter> filters = Lists.newArrayList(new Filter().withCondition("IS").withKey("PredictorArn").withValue(predictorArn));
            if (StringUtils.isNotBlank(status)) {
                filters.add(new Filter().withCondition("IS").withKey("Status").withValue(status));
            }
            listForecastsRequest.setFilters(filters);

            if (nextToken != null) {
                listForecastsRequest.setNextToken(nextToken);
            }
            ListForecastsResult listForecastsResult = forecastClient.listForecasts(listForecastsRequest);

            existingForecasts.addAll(listForecastsResult.getForecasts());
            nextToken = listForecastsResult.getNextToken();
        } while (nextToken != null);

        return existingForecasts;
    }
}
