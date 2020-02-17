package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.services.forecast.AmazonForecast;
import com.amazonaws.services.forecast.model.CreateDatasetGroupRequest;
import com.amazonaws.services.forecast.model.ResourceAlreadyExistsException;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Map;

import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_ARN_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_GROUP_NAME_KEY;

@Slf4j
public class CreateDatasetGroupHandler extends AbstractPredictionGenerationLambdaHandler {

    private static final String DATASET_GROUP_RESOURCE_TYPE = "datasetGroup";

    public CreateDatasetGroupHandler() {
        super();
    }

    public CreateDatasetGroupHandler(final AmazonForecast forecastClient) {
        super(forecastClient);
    }

    @Override
    public void process(final Map<String, String> resourceIdMap) {

        String datasetArn = resourceIdMap.get(DATASET_ARN_KEY);
        String datasetGroupName = resourceIdMap.get(DATASET_GROUP_NAME_KEY);
        log.info(String.format("The datasetArn and %s getting from resourceIdMap are [%s] and [%s]",
                DATASET_GROUP_RESOURCE_TYPE, datasetArn, datasetGroupName));

        /*
         * Create the datasetGroup, since this API call is synchronized,
         * once it returns 200, we know the corresponding datasetGroup is created successfully.
         * So we don't need to describe for the datasetGroup status.
         * Also, refer to: https://docs.aws.amazon.com/forecast/latest/dg/API_DescribeDatasetGroup.html
         * datasetGroup doesn't even have the 'Status' attribute.
         */
        try {
            createDatasetGroup(datasetArn, datasetGroupName, DOMAIN);
        } catch (ResourceAlreadyExistsException e) {
            log.info(String.format("The %s [%s] already exists.", DATASET_GROUP_RESOURCE_TYPE, datasetGroupName));
        }

        log.info(String.format("Successfully setup the %s %s", DATASET_GROUP_RESOURCE_TYPE, datasetGroupName));
    }

    private void createDatasetGroup(final String datasetArn,
                                    final String datasetGroupName,
                                    final String domain) {
        CreateDatasetGroupRequest createDatasetGroupRequest = new CreateDatasetGroupRequest();
        createDatasetGroupRequest.setDatasetArns(Collections.singletonList(datasetArn));
        createDatasetGroupRequest.setDatasetGroupName(datasetGroupName);
        createDatasetGroupRequest.setDomain(domain);
        forecastClient.createDatasetGroup(createDatasetGroupRequest);
    }
}
