package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.dagger.DaggerLambdaFunctionsComponent;
import com.amazonaws.lambda.predictiongeneration.exception.ResourceSetupFailureException;
import com.amazonaws.lambda.predictiongeneration.exception.ResourceSetupInProgressException;
import com.amazonaws.services.forecast.AmazonForecast;
import com.amazonaws.services.forecast.model.DatasetSummary;
import com.amazonaws.services.forecast.model.PredictorSummary;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.RESOURCE_ACTIVE_STATUS;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.RESOURCE_FAILED_STATUS;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.listDatasets;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.listPredictors;

@Slf4j
public abstract class AbstractPredictionGenerationLambdaHandler implements RequestHandler<String, String> {

    // Will be used for CreateDataset and CreateDatasetGroup APIs
    protected static final String DOMAIN = "CUSTOM";

    @Inject
    @NonNull
    protected AmazonForecast forecastClient;

    AbstractPredictionGenerationLambdaHandler() {
        DaggerLambdaFunctionsComponent.create().inject(this);
    }

    AbstractPredictionGenerationLambdaHandler(final AmazonForecast forecastClient) {
        this.forecastClient = forecastClient;
    }

    @Override
    public String handleRequest(final String input, Context context) {
        Map<String, String> resourceIdMap;
        try {
            resourceIdMap = new ObjectMapper().readValue(input, new TypeReference<Map<String, String>>() {
            });
        } catch(IOException e) {
            String errorMsg = e.getMessage();
            log.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }
        process(resourceIdMap);
        return input;
    }

    abstract void process(Map<String, String> resourceIdMap);

    /**
     * @return true if status is ACTIVE, and it indicates the resource setup is successfully finished.
     * @throws ResourceSetupInProgressException if resourceStatus is FAILED
     * @throws ResourceSetupFailureException otherwise
     */
    protected boolean takeActionByResourceStatus(final String resourceStatus,
                                                 final String resourceType,
                                                 final String resourceName)
            throws ResourceSetupFailureException, ResourceSetupInProgressException {

        switch (resourceStatus) {
            case RESOURCE_ACTIVE_STATUS:
                log.info(String.format("Successfully created %s %s: [%s]", RESOURCE_ACTIVE_STATUS, resourceType, resourceName));
                return true;

            case RESOURCE_FAILED_STATUS:
                throw new ResourceSetupFailureException(String.format("%s: [%s] setup failed.", resourceType, resourceName));

            default:
                throw new ResourceSetupInProgressException(
                        String.format("%s: [%s] setup is in progress with current status [%s]",
                                resourceType, resourceName, resourceStatus));
        }
    }

    protected List<String> listOutdatedDatasetArns(final String currentDatasetArn) {
        List<String> existingDatasetArns = listDatasetArns();
        existingDatasetArns.remove(currentDatasetArn);
        return existingDatasetArns;
    }

    private List<String> listDatasetArns() {
        return listDatasets(forecastClient).stream().map(DatasetSummary::getDatasetArn).collect(Collectors.toList());
    }

    protected List<String> listOutdatedPredictorArns(final String currentPredictorArn) {
        List<String> existingPredictorArns = listPredictorArns();
        existingPredictorArns.remove(currentPredictorArn);
        return existingPredictorArns;
    }

    private List<String> listPredictorArns() {
        List<PredictorSummary> existingPredictors = listPredictors(forecastClient);
        return existingPredictors.stream().map(PredictorSummary::getPredictorArn).collect(Collectors.toList());
    }
}
