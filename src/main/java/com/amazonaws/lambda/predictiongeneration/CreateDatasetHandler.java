package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.services.forecast.AmazonForecast;
import com.amazonaws.services.forecast.model.CreateDatasetRequest;
import com.amazonaws.services.forecast.model.DescribeDatasetRequest;
import com.amazonaws.services.forecast.model.ResourceNotFoundException;
import com.amazonaws.services.forecast.model.Schema;
import com.amazonaws.services.forecast.model.SchemaAttribute;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_ARN_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_NAME_KEY;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATA_FREQUENCY_KEY;

@Slf4j
public class CreateDatasetHandler extends AbstractPredictionGenerationLambdaHandler {

    private static final String DATASET_TYPE = "TARGET_TIME_SERIES";
    private static final String DATASET_RESOURCE_TYPE = "dataset";

    public CreateDatasetHandler() {
        super();
    }

    public CreateDatasetHandler(AmazonForecast forecastClient) {
        super(forecastClient);
    }

    @Override
    public void process(final Map<String, String> resourceIdMap) {

        final String datasetName = resourceIdMap.get(DATASET_NAME_KEY);
        final String datasetArn = resourceIdMap.get(DATASET_ARN_KEY);
        final String dataFrequency = resourceIdMap.get(DATA_FREQUENCY_KEY);
        log.info(String.format("The %s and dataFrequency getting from resourceIdMap are [%s] and [%s]",
                DATASET_RESOURCE_TYPE, datasetArn, dataFrequency));

        // Check if dataset exists
        try {
            String currentStatus = describeDatasetStatus(datasetArn);
            if (takeActionByResourceStatus(currentStatus, DATASET_RESOURCE_TYPE, datasetArn)) {
                return;
            }
        } catch (ResourceNotFoundException e) {
            log.info(String.format("Cannot find %s with arn [%s]. Proceed to create a new one", DATASET_RESOURCE_TYPE, datasetArn));
        }

        // Create the dataset if found no matching dataset name
        createDataset(DOMAIN, DATASET_TYPE, datasetName, dataFrequency);
        log.info("finish triggering CreateDatasetCall.");

        String newStatus = describeDatasetStatus(datasetArn);
        takeActionByResourceStatus(newStatus, DATASET_RESOURCE_TYPE, datasetArn);
    }

    private void createDataset(final String domain,
                               final String datasetType,
                               final String datasetName,
                               final String dataFrequency) {
        CreateDatasetRequest createDatasetRequest = new CreateDatasetRequest();
        createDatasetRequest.setDomain(domain);
        createDatasetRequest.setDatasetType(datasetType);
        createDatasetRequest.setDatasetName(datasetName);
        createDatasetRequest.setDataFrequency(dataFrequency);

        // schema configuration
        List<SchemaAttribute> schemaAttributes = Lists.newArrayList(

                // Refer to https://docs.aws.amazon.com/forecast/latest/dg/API_CreateDataset.html#forecast-CreateDataset-request-Schema
                // The schema attributes and their order must match the fields in your training data file.
                new SchemaAttribute().withAttributeName("item_id").withAttributeType("string"),
                new SchemaAttribute().withAttributeName("timestamp").withAttributeType("timestamp"),
                new SchemaAttribute().withAttributeName("target_value").withAttributeType("integer")
        );
        Schema schema = new Schema().withAttributes(schemaAttributes);
        createDatasetRequest.setSchema(schema);
        forecastClient.createDataset(createDatasetRequest);
    }

    private String describeDatasetStatus(final String datasetArn) {
        DescribeDatasetRequest describeDatasetRequest = new DescribeDatasetRequest();
        describeDatasetRequest.setDatasetArn(datasetArn);
        return forecastClient.describeDataset(describeDatasetRequest).getStatus();
    }
}
