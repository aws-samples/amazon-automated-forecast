package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.lambda.predictiongeneration.exception.ResourceCleanupInProgressException;
import com.amazonaws.services.forecast.AmazonForecast;
import com.amazonaws.services.forecast.model.DeletePredictorRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Map;

import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.PREDICTOR_ARN_KEY;

@Slf4j
public class DeleteOutdatedPredictorsHandler extends AbstractPredictionGenerationLambdaHandler{

    public DeleteOutdatedPredictorsHandler() {
        super();
    }

    public DeleteOutdatedPredictorsHandler(final AmazonForecast forecastClient) {
        super(forecastClient);
    }

    @Override
    public void process(final Map<String, String> resourceIdMap) {

        String preservedPredictorArn = resourceIdMap.get(PREDICTOR_ARN_KEY);
        log.info(String.format("The preserved predictorArn getting from resourceIdMap is [%s]", preservedPredictorArn));

        // Get all existing predictors and exclude the preserved one
        List<String> outdatedPredictors = listOutdatedPredictorArns(preservedPredictorArn);

        if (outdatedPredictors.isEmpty()) {
            log.info("Don't find any outdated predictor, returning");
            return;
        }

        // Delete all outdated predictors
        for (String outdatedPredictorArn : outdatedPredictors) {
            deletePredictor(outdatedPredictorArn);
        }

        // Verify there is no outdated predictors
        List<String> outdatedPredictorArnsAfterCleanup = listOutdatedPredictorArns(preservedPredictorArn);
        if (CollectionUtils.isNotEmpty(outdatedPredictorArnsAfterCleanup)) {
            throw new ResourceCleanupInProgressException(
                    String.format("Outdated predictors cleanup is in progress with outdated predictors [%s]",
                            outdatedPredictorArnsAfterCleanup.toString()));
        }

        log.info("Successfully clean up outdated predictors.");
    }

    private void deletePredictor(final String predictorArn) {
        DeletePredictorRequest deletePredictorRequest = new DeletePredictorRequest();
        deletePredictorRequest.setPredictorArn(predictorArn);
        forecastClient.deletePredictor(deletePredictorRequest);
    }

}
