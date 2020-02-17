package com.amazonaws.lambda.queryingpredictionresult;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.Rule;
import org.mockito.Mock;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.contrib.java.lang.system.EnvironmentVariables;

import static com.amazonaws.lambda.queryingpredictionresult.LoadDataFromS3ToDynamoDBHandler.DYNAMODB_PREDICTION_METADATA_LATEST_PRED_DATA_FREQ_IN_SEC_ATTR_NAME;
import static com.amazonaws.lambda.queryingpredictionresult.LoadDataFromS3ToDynamoDBHandler.DYNAMODB_PREDICTION_METADATA_LATEST_PRED_UUID_ATTR_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LoadDataFromS3ToDynamoDBHandlerTest {

    private static final String AWS_REGION = "us-east-1";
    private static final String UNIT_TEST_ROOT_CLASS_PATH = "/";
    private static final String UNIT_TEST_S3_FOLDER_NAME = "target";
    private static final String PREDICTION_TABLE_NAME = "LocalTestTable";
    private static final String PREDICTION_TABLE_HASH_KEY = PredictionResultItem.Attribute.ITEM_ID;
    private static final String PREDICTION_TABLE_RANGE_KEY = PredictionResultItem.Attribute.DATE;
    private static final String FORECAST_HORIZON_IN_DAYS = "3";
    private static final String PREDICTION_METADATA_TABLE_NAME = "PredictionMetadata";
    private static final String PREDICTION_METADATA_TABLE_HASH_KEY = "metadataKey";
    private static final String PREDICTION_METADATA_TABLE_ATTRIBUTE_NAME = "metadataValue";

    // test csv files are located under folder: resources/target/
    private static final String TEST_EMPTY_FORECAST_EXPORT_JOB = "empty_forecast_export_job";
    private static final String TEST_OBJECT_KEY0 = String.format("%s/%s_2019-10-16T21-40-00Z_part0.csv", UNIT_TEST_S3_FOLDER_NAME, TEST_EMPTY_FORECAST_EXPORT_JOB);

    private static final String TEST_FORECAST_EXPORT_JOB1 = "forecast_export_job1";
    private static final String TEST_OBJECT_KEY1 = String.format("%s/%s_2019-10-16T21-40-00Z_part0.csv", UNIT_TEST_S3_FOLDER_NAME, TEST_FORECAST_EXPORT_JOB1);
    private static final long TEST_PREDICTION1_DATA_FREQUENCY_IN_SECONDS = 3600L;

    private static final String TEST_FORECAST_EXPORT_JOB_WITH_ONE_RECORD = "forecast_export_job_with_one_record";
    private static final String TEST_OBJECT_KEY2 = String.format("%s/%s_2019-10-16T21-40-00Z_part0.csv", UNIT_TEST_S3_FOLDER_NAME, TEST_FORECAST_EXPORT_JOB_WITH_ONE_RECORD);

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Mock
    private Context context;

    private AmazonS3 mockS3Client;
    private AmazonDynamoDB localDdbClient;
    private LoadDataFromS3ToDynamoDBHandler handler;

    @BeforeEach
    void setup() {

        // Setup Env variables
        environmentVariables.set("AWS_REGION", AWS_REGION);
        environmentVariables.set("PREDICTION_TABLE_NAME", PREDICTION_TABLE_NAME);
        environmentVariables.set("PREDICTION_TABLE_HASH_KEY", PREDICTION_TABLE_HASH_KEY);
        environmentVariables.set("PREDICTION_TABLE_RANGE_KEY", PREDICTION_TABLE_RANGE_KEY);
        environmentVariables.set("FORECAST_HORIZON_IN_DAYS", FORECAST_HORIZON_IN_DAYS);
        environmentVariables.set("PREDICTION_METADATA_TABLE_NAME", PREDICTION_METADATA_TABLE_NAME);
        environmentVariables.set("PREDICTION_METADATA_TABLE_HASH_KEY", PREDICTION_METADATA_TABLE_HASH_KEY);
        environmentVariables.set("PREDICTION_METADATA_TABLE_ATTRIBUTE_NAME", PREDICTION_METADATA_TABLE_ATTRIBUTE_NAME);

        mockS3Client = initMockS3Client();
        localDdbClient = initLocalDynamoDB();
        handler = new LoadDataFromS3ToDynamoDBHandler(
                mockS3Client,
                localDdbClient
        );
    }

    @AfterEach
    void tearDown() {
        localDdbClient.deleteTable(PREDICTION_METADATA_TABLE_NAME);
        localDdbClient.deleteTable(PREDICTION_TABLE_NAME);
        localDdbClient = null;
        mockS3Client = null;
        handler = null;
    }

    @Test
    public void testLoadDataFromS3ToDynamoDB_WithThrowingCannotParseException() {
        String dummyS3Key = "dummy";
        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> handler.handleRequest(makeMockS3Event("dummy"), context));

        assertEquals(String.format("Cannot parse prediction result object key: %s", dummyS3Key), thrown.getMessage());
    }

    @Test
    public void testLoadDataFromS3ToDynamoDB_WithEmptyPredictionResultFile() {
        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> handler.handleRequest(makeMockS3Event(TEST_OBJECT_KEY0), context));

        assertEquals(String.format("Prediction result file %s contains no record.", TEST_OBJECT_KEY0), thrown.getMessage());

        // cleanup
        refreshLocalDynamoDB();
    }

    @Test
    public void testLoadDataFromS3ToDynamoDB() throws IOException {
        handler.handleRequest(makeMockS3Event(TEST_OBJECT_KEY1), context);
        verifyDynamoDB(TEST_FORECAST_EXPORT_JOB1);

        // cleanup
        refreshLocalDynamoDB();
    }

    @Test
    public void testLoadDataFromS3ToDynamoDB_WithOneRecordPredictionResultFile() {
        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> handler.handleRequest(makeMockS3Event(TEST_OBJECT_KEY2), context));

        assertEquals(String.format("Passed in items contains %d item, which is less than 2.", 1), thrown.getMessage());

        // cleanup
        refreshLocalDynamoDB();
    }

    private void verifyDynamoDB(final String forecastExportJobName) throws IOException {

        // Verify the latestPredictionUUID in metadata table
        Map<String, AttributeValue> latestPredictionUuidHashKey = new HashMap<>();
        latestPredictionUuidHashKey.put(PREDICTION_METADATA_TABLE_HASH_KEY,
                new AttributeValue(DYNAMODB_PREDICTION_METADATA_LATEST_PRED_UUID_ATTR_NAME));
        GetItemRequest getUuidItemRequest = new GetItemRequest()
                .withTableName(PREDICTION_METADATA_TABLE_NAME)
                .withKey(latestPredictionUuidHashKey);
        GetItemResult getUuidItemResult = localDdbClient.getItem(getUuidItemRequest);
        assertEquals(new AttributeValue(forecastExportJobName), getUuidItemResult.getItem().get(PREDICTION_METADATA_TABLE_ATTRIBUTE_NAME));

        // Verify the latestPredictionDataFrequency in metadata table
        Map<String, AttributeValue> latestPredictionDataFreqHashKey = new HashMap<>();
        latestPredictionUuidHashKey.put(PREDICTION_METADATA_TABLE_HASH_KEY,
                new AttributeValue(DYNAMODB_PREDICTION_METADATA_LATEST_PRED_DATA_FREQ_IN_SEC_ATTR_NAME));
        GetItemRequest getDataFreqItemRequest = new GetItemRequest()
                .withTableName(PREDICTION_METADATA_TABLE_NAME)
                .withKey(latestPredictionUuidHashKey);
        GetItemResult getDataFreqItemResult = localDdbClient.getItem(getDataFreqItemRequest);
        assertEquals(new AttributeValue(String.valueOf(TEST_PREDICTION1_DATA_FREQUENCY_IN_SECONDS)),
                getDataFreqItemResult.getItem().get(PREDICTION_METADATA_TABLE_ATTRIBUTE_NAME));

        // Verify the number of items in PredictionResultItem table
        ScanRequest predictionTableScanRequest = new ScanRequest()
                .withTableName(PREDICTION_TABLE_NAME);
        ScanResult predictionTableScanResult = localDdbClient.scan(predictionTableScanRequest);
        long itemCount = getNumberOfLines(TEST_OBJECT_KEY1) - 1;
        assertEquals(itemCount, predictionTableScanResult.getItems().size());
    }

    private long getNumberOfLines(final String fileName) throws IOException {
        final InputStream inputStream = getClass().getResourceAsStream(UNIT_TEST_ROOT_CLASS_PATH + fileName);
        long lines = 0;
        try (LineNumberReader lnr = new LineNumberReader(new InputStreamReader(inputStream))) {
            while (lnr.readLine() != null) {
                lines++;
            }
        }

        return lines;
    }

    private S3Event makeMockS3Event(final String objectKey) {

        S3EventNotification.S3BucketEntity bucket = new S3EventNotification.S3BucketEntity("dummyBucket",
                mock(S3EventNotification.UserIdentityEntity.class), "dummyArn");
        S3EventNotification.S3ObjectEntity object = new S3EventNotification.S3ObjectEntity(objectKey, 1024L, "dummyEtag", "dummyVersionId");
        S3EventNotification.S3Entity s3 = new S3EventNotification.S3Entity("dummyConfigurationId",
                bucket, object, "dummySchemaVer");

        S3EventNotificationRecord record = new S3EventNotificationRecord("dummyRegion",
                "dummyEventName", "dummyEventSrc", "2019-01-01T00:00:00.000",
                "dummyEventVer", mock(S3EventNotification.RequestParametersEntity.class),
                mock(S3EventNotification.ResponseElementsEntity.class), s3, mock(S3EventNotification.UserIdentityEntity.class));

        return new S3Event(Collections.singletonList(record));
    }

    private void refreshLocalDynamoDB() {
        // first delete the tables
        localDdbClient.deleteTable(PREDICTION_METADATA_TABLE_NAME);
        localDdbClient.deleteTable(PREDICTION_TABLE_NAME);

        // then recreate them
        localDdbClient = initLocalDynamoDB();
    }

    private AmazonDynamoDB initLocalDynamoDB() {
        AmazonDynamoDB localDdbClient = DynamoDBEmbedded.create().amazonDynamoDB();
        ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
                .withReadCapacityUnits(200L)
                .withWriteCapacityUnits(200L);

        // create local prediction result table
        List<KeySchemaElement> predictionResultTableKeys = new ArrayList<>();
        predictionResultTableKeys.add(new KeySchemaElement().withAttributeName(PREDICTION_TABLE_HASH_KEY).withKeyType(KeyType.HASH));
        predictionResultTableKeys.add(new KeySchemaElement().withAttributeName(PREDICTION_TABLE_RANGE_KEY).withKeyType(KeyType.RANGE));
        List<AttributeDefinition> predictionResultTableAttrs = new ArrayList<>();
        predictionResultTableAttrs.add(new AttributeDefinition().withAttributeName(PREDICTION_TABLE_HASH_KEY).withAttributeType(ScalarAttributeType.S));
        predictionResultTableAttrs.add(new AttributeDefinition().withAttributeName(PREDICTION_TABLE_RANGE_KEY).withAttributeType(ScalarAttributeType.S));
        CreateTableRequest predictionResultCreateTableRequest = new CreateTableRequest()
                .withTableName(PREDICTION_TABLE_NAME)
                .withKeySchema(predictionResultTableKeys)
                .withAttributeDefinitions(predictionResultTableAttrs)
                .withProvisionedThroughput(provisionedThroughput);
        localDdbClient.createTable(predictionResultCreateTableRequest);

        // create local prediction metadata table
        KeySchemaElement predictionMetadataTableKey = new KeySchemaElement()
                .withAttributeName(PREDICTION_METADATA_TABLE_HASH_KEY)
                .withKeyType(KeyType.HASH);
        AttributeDefinition predictionMetadataTableAttr = new AttributeDefinition()
                .withAttributeName(PREDICTION_METADATA_TABLE_HASH_KEY)
                .withAttributeType(ScalarAttributeType.S);
        CreateTableRequest predictionMetadataCreateTableRequest = new CreateTableRequest()
                .withTableName(PREDICTION_METADATA_TABLE_NAME)
                .withKeySchema(Collections.singletonList(predictionMetadataTableKey))
                .withAttributeDefinitions(Collections.singletonList(predictionMetadataTableAttr))
                .withProvisionedThroughput(provisionedThroughput);
        localDdbClient.createTable(predictionMetadataCreateTableRequest);

        return localDdbClient;
    }

    private AmazonS3 initMockS3Client() {
        AmazonS3 mockS3Client = mock(AmazonS3.class);
        when(mockS3Client.getObject(any(GetObjectRequest.class))).thenAnswer(
                invocationOnMock -> {
                    GetObjectRequest req = invocationOnMock.getArgument(0);

                    try {
                        return mockS3ObjectFromLocalFile(req.getKey());
                    } catch (NullPointerException e) {
                        // Any request that cannot find match key, we should throw an S3 Exception
                        throw new AmazonS3Exception("Object not found or not available");
                    }
                }
        );
        return mockS3Client;
    }

    private S3Object mockS3ObjectFromLocalFile(String fileName) {

        final InputStream inputStream = getClass().getResourceAsStream(UNIT_TEST_ROOT_CLASS_PATH + fileName);
        if (inputStream == null) {
            throw new NullPointerException();
        }
        S3Object s3Object = mock(S3Object.class);

        // mock an S3ObjectInputStream (stream returned from S3 GET response)
        S3ObjectInputStream mockS3ObjectInputStream = new S3ObjectInputStream(inputStream, null);

        when(s3Object.getObjectContent()).thenReturn(mockS3ObjectInputStream);
        return s3Object;
    }
}
