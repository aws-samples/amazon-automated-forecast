package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.lambda.predictiongeneration.exception.ResourceCleanupInProgressException;
import com.amazonaws.services.forecast.AmazonForecast;
import com.amazonaws.services.forecast.model.DeleteDatasetRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Map;

import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_ARN_KEY;

@Slf4j
public class DeleteOutdatedDatasetsHandler extends AbstractPredictionGenerationLambdaHandler {

    public DeleteOutdatedDatasetsHandler() {
        super();
    }

    public DeleteOutdatedDatasetsHandler(final AmazonForecast forecastClient) {
        super(forecastClient);
    }

    @Override
    public void process(final Map<String, String> resourceIdMap) {

        String preservedDatasetArn = resourceIdMap.get(DATASET_ARN_KEY);
        log.info(String.format("The preserved datasetArn getting from resourceIdMap is [%s]", preservedDatasetArn));

        // Get all existing datasets and exclude the preserved one
        List<String> outdatedDatasetArns = listOutdatedDatasetArns(preservedDatasetArn);

        if (outdatedDatasetArns.isEmpty()) {
            log.info("Don't find any outdated dataset, returning");
            return;
        }

        // Delete all outdated datasets
        for (String outdatedDatasetArn : outdatedDatasetArns) {
            deleteDataset(outdatedDatasetArn);
        }

        // Verify there is no outdated datasets
        List<String> outdatedDatasetsAfterCleanup = listOutdatedDatasetArns(preservedDatasetArn);
        if (CollectionUtils.isNotEmpty(outdatedDatasetsAfterCleanup)) {
            throw new ResourceCleanupInProgressException(
                    String.format("Outdated datasets cleanup is in progress with outdated datasets [%s]",
                            outdatedDatasetsAfterCleanup.toString()));
        }

        log.info("Successfully clean up outdated datasets.");
    }

    private void deleteDataset(final String datasetArn) {
        DeleteDatasetRequest deleteDatasetRequest = new DeleteDatasetRequest();
        deleteDatasetRequest.setDatasetArn(datasetArn);
        forecastClient.deleteDataset(deleteDatasetRequest);
    }
}
