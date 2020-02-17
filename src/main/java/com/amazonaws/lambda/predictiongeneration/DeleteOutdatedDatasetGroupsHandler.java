package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.lambda.predictiongeneration.exception.ResourceCleanupInProgressException;
import com.amazonaws.services.forecast.AmazonForecast;
import com.amazonaws.services.forecast.model.DatasetGroupSummary;
import com.amazonaws.services.forecast.model.DeleteDatasetGroupRequest;
import com.amazonaws.services.forecast.model.ListDatasetGroupsRequest;
import com.amazonaws.services.forecast.model.ListDatasetGroupsResult;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_GROUP_ARN_KEY;

@Slf4j
public class DeleteOutdatedDatasetGroupsHandler extends AbstractPredictionGenerationLambdaHandler {

    public DeleteOutdatedDatasetGroupsHandler() {
        super();
    }

    public DeleteOutdatedDatasetGroupsHandler(final AmazonForecast forecastClient) {
        super(forecastClient);
    }

    @Override
    public void process(final Map<String, String> resourceIdMap) {

        String preservedDatasetGroupArn = resourceIdMap.get(DATASET_GROUP_ARN_KEY);
        log.info(String.format("The preserved datasetGroupArn getting from resourceIdMap is %s", preservedDatasetGroupArn));

        // Get all existing datasetGroups and exclude the preserved one
        List<String> outdatedDatasetGroups = listDatasetGroupArns();

        if (outdatedDatasetGroups.isEmpty()) {
            throw new IllegalStateException("There is no existing datasetGroup.");
        }

        outdatedDatasetGroups.remove(preservedDatasetGroupArn);
        if (outdatedDatasetGroups.isEmpty()) {
            log.info("Don't find any outdated datasetGroup, returning");
            return;
        }

        // Delete all outdated datasetGroups
        for (String outdatedDatasetGroupArn : outdatedDatasetGroups) {
            deleteDatasetGroup(outdatedDatasetGroupArn);
        }

        // Verify there is no outdated datasetGroups
        List<String> existingDatasetGroups = listDatasetGroupArns();
        if (!Collections.singletonList(preservedDatasetGroupArn).equals(existingDatasetGroups)) {
            throw new ResourceCleanupInProgressException(
                    String.format("Outdated datasetGroups cleanup is in progress with existing datasetGroups %s",
                            existingDatasetGroups.toString()));
        }

        log.info("Successfully clean up outdated datasetGroups.");
    }

    private void deleteDatasetGroup(final String datasetGroupArn) {
        DeleteDatasetGroupRequest deleteDatasetGroupRequest = new DeleteDatasetGroupRequest();
        deleteDatasetGroupRequest.setDatasetGroupArn(datasetGroupArn);
        forecastClient.deleteDatasetGroup(deleteDatasetGroupRequest);
    }

    private List<String> listDatasetGroupArns() {
        List<String> existingDatasetGroups = new ArrayList<>();
        String nextToken = null;
        do {
            ListDatasetGroupsRequest listDatasetGroupsRequest = new ListDatasetGroupsRequest();
            if (nextToken != null) {
                listDatasetGroupsRequest.setNextToken(nextToken);
            }
            ListDatasetGroupsResult listDatasetGroupsResult = forecastClient.listDatasetGroups(listDatasetGroupsRequest);

            existingDatasetGroups.addAll(
                    listDatasetGroupsResult.getDatasetGroups().stream()
                            .map(DatasetGroupSummary::getDatasetGroupArn).collect(Collectors.toList()));
            nextToken = listDatasetGroupsResult.getNextToken();
        } while (nextToken != null);

        return existingDatasetGroups;
    }
}
