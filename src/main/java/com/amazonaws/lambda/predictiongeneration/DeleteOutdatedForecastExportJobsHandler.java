package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.lambda.predictiongeneration.exception.ResourceCleanupInProgressException;
import com.amazonaws.services.forecast.AmazonForecast;
import com.amazonaws.services.forecast.model.DeleteForecastExportJobRequest;
import com.amazonaws.services.forecast.model.Filter;
import com.amazonaws.services.forecast.model.FilterConditionString;
import com.amazonaws.services.forecast.model.ForecastExportJobSummary;
import com.amazonaws.services.forecast.model.ListForecastExportJobsRequest;
import com.amazonaws.services.forecast.model.ListForecastExportJobsResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.FORECAST_ARN_KEY;

@Slf4j
public class DeleteOutdatedForecastExportJobsHandler extends AbstractPredictionGenerationLambdaHandler {

    public DeleteOutdatedForecastExportJobsHandler() {
        super();
    }

    public DeleteOutdatedForecastExportJobsHandler(AmazonForecast forecastClient) {
        super(forecastClient);
    }

    @Override
    public void process(final Map<String, String> resourceIdMap) {
        String preservedForecastArn = resourceIdMap.get(FORECAST_ARN_KEY);
        log.info(String.format("The preservedForecastArn getting from resourceIdMap is [%s]", preservedForecastArn));

        // Get all existing datasetImportJobs and exclude the ones associated with the preserved dataset name
        List<String> outdatedForecastExportJobArns = listOutdatedForecastExportJobArns(preservedForecastArn);

        if (CollectionUtils.isEmpty(outdatedForecastExportJobArns)) {
            log.info("Don't find any outdated forecast export job, returning");
            return;
        }

        // Delete all forecastExportJobs associated with outdated forecasts
        outdatedForecastExportJobArns.forEach(this::deleteForecastExportJob);

        // Verify there is no outdated forecastExportJobs
        List<String> outdatedForecastExportJobArnsAfterCleanup = listOutdatedForecastExportJobArns(preservedForecastArn);
        if (CollectionUtils.isNotEmpty(outdatedForecastExportJobArnsAfterCleanup)) {
            throw new ResourceCleanupInProgressException(
                    String.format("Outdated forecastExportJobs cleanup is in progress with outdated forecastExportJobs [%s]",
                            outdatedForecastExportJobArnsAfterCleanup));
        }

        log.info("Successfully clean up outdated forecastExportJobs.");
    }

    private void deleteForecastExportJob(final String forecastExportJobArn) {
        forecastClient.deleteForecastExportJob(new DeleteForecastExportJobRequest().withForecastExportJobArn(forecastExportJobArn));
    }

    private List<String> listOutdatedForecastExportJobArns(final String preservedForecastArn) {

        List<String> outdatedForecastExportJobArns = new ArrayList<>();
        String nextToken = null;
        ListForecastExportJobsRequest listForecastExportJobsRequest =
                new ListForecastExportJobsRequest().withFilters(
                        new Filter()
                                .withKey("ForecastArn")
                                .withValue(preservedForecastArn)
                                .withCondition(FilterConditionString.IS_NOT));

        do {
            if (nextToken != null) {
                listForecastExportJobsRequest.setNextToken(nextToken);
            }
            ListForecastExportJobsResult listForecastExportJobsResult = forecastClient
                    .listForecastExportJobs(listForecastExportJobsRequest);

            outdatedForecastExportJobArns.addAll(listForecastExportJobsResult.getForecastExportJobs()
                    .stream().map(ForecastExportJobSummary::getForecastExportJobArn).collect(Collectors.toList()));
            nextToken = listForecastExportJobsResult.getNextToken();
        } while (nextToken != null);

        return outdatedForecastExportJobArns;
    }
}
