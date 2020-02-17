package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.lambda.predictiongeneration.exception.ResourceCleanupInProgressException;
import com.amazonaws.services.forecast.AmazonForecast;
import com.amazonaws.services.forecast.model.DatasetImportJobSummary;
import com.amazonaws.services.forecast.model.DeleteDatasetImportJobRequest;
import com.amazonaws.services.forecast.model.Filter;
import com.amazonaws.services.forecast.model.FilterConditionString;
import com.amazonaws.services.forecast.model.ListDatasetImportJobsRequest;
import com.amazonaws.services.forecast.model.ListDatasetImportJobsResult;
import com.amazonaws.services.forecast.model.ResourceNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_ARN_KEY;

@Slf4j
public class DeleteOutdatedDatasetImportJobsHandler extends AbstractPredictionGenerationLambdaHandler {

    public DeleteOutdatedDatasetImportJobsHandler() {
        super();
    }

    public DeleteOutdatedDatasetImportJobsHandler(final AmazonForecast forecastClient) {
        super(forecastClient);
    }

    @Override
    public void process(final Map<String, String> resourceIdMap) {

        String preservedDatasetArn = resourceIdMap.get(DATASET_ARN_KEY);
        log.info(String.format("The preserved datasetArn getting from resourceIdMap is [%s]", preservedDatasetArn));

        // Get all existing datasetImportJobs and exclude the ones associated with the preserved dataset name
        Map<String, List<String>> outdatedDatasetImportJobsMap = listOutdatedDatasetImportJobArns(preservedDatasetArn);

        if (CollectionUtils.isEmpty(outdatedDatasetImportJobsMap.keySet())) {
            log.info("Don't find any outdated dataset import job, returning");
            return;
        }

        // Delete all datasetImportJobs associated with outdated datasets
        outdatedDatasetImportJobsMap.values().stream().flatMap(List::stream).forEach(this::deleteDatasetImportJob);

        // Verify there is no outdated datasetImportJobs
        Map<String, List<String>> outdatedDatasetImportJobsMapAfterCleanup = listOutdatedDatasetImportJobArns(preservedDatasetArn);
        if (CollectionUtils.isNotEmpty(outdatedDatasetImportJobsMapAfterCleanup.keySet())) {
            throw new ResourceCleanupInProgressException(
                    String.format("Outdated datasetImportJobs cleanup is in progress with outdated datasetImportJobs [%s]",
                            outdatedDatasetImportJobsMapAfterCleanup.keySet().toString()));
        }

        log.info("Successfully clean up outdated datasetImportJobs.");
    }

    private void deleteDatasetImportJob(final String datasetImportJobArn) {
        DeleteDatasetImportJobRequest deleteDatasetImportJobRequest =
                new DeleteDatasetImportJobRequest().withDatasetImportJobArn(datasetImportJobArn);

        log.info(String.format("About to delete datasetImportJob: %s", datasetImportJobArn));

        try {
            forecastClient.deleteDatasetImportJob(deleteDatasetImportJobRequest);
        } catch (ResourceNotFoundException ex) {
            log.info(String.format("DatasetImportJob [%s] has already been deleted", datasetImportJobArn));
        }
    }

    private Map<String, List<String>> listOutdatedDatasetImportJobArns(final String preservedDatasetArn) {
        List<String> outdatedDatasetArns = listOutdatedDatasetArns(preservedDatasetArn);

        Map<String, List<String>> outdatedDatasetImportJobsMap = new HashMap<>();

        for (String outdatedDatasetArn : outdatedDatasetArns) {
            List<String> outdatedDatasetImportJobArns = new ArrayList<>();
            String nextToken = null;
            ListDatasetImportJobsRequest listDatasetImportJobsRequest =
                    new ListDatasetImportJobsRequest().withFilters(
                            new Filter()
                                    .withKey("DatasetArn")
                                    .withValue(outdatedDatasetArn)
                                    .withCondition(FilterConditionString.IS));

            do {
                if (nextToken != null) {
                    listDatasetImportJobsRequest.setNextToken(nextToken);
                }
                ListDatasetImportJobsResult listDatasetImportJobsResult = forecastClient
                        .listDatasetImportJobs(listDatasetImportJobsRequest);

                outdatedDatasetImportJobArns.addAll(listDatasetImportJobsResult.getDatasetImportJobs()
                        .stream().map(DatasetImportJobSummary::getDatasetImportJobArn).collect(Collectors.toList()));
                nextToken = listDatasetImportJobsResult.getNextToken();
            } while (nextToken != null);

            if (!outdatedDatasetImportJobArns.isEmpty()) {
                outdatedDatasetImportJobsMap.put(outdatedDatasetArn, outdatedDatasetImportJobArns);
            }
        }

        return outdatedDatasetImportJobsMap;
    }
}
