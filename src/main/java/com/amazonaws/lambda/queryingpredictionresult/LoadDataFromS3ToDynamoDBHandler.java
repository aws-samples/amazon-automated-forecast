package com.amazonaws.lambda.queryingpredictionresult;

import com.amazonaws.dagger.DaggerLambdaFunctionsComponent;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.Put;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RequiredArgsConstructor
@Slf4j
public class LoadDataFromS3ToDynamoDBHandler implements RequestHandler<S3Event, Void> {

    private static final String DYNAMODB_PREDICTION_TABLE_NAME = System.getenv("PREDICTION_TABLE_NAME");
    private static final String DYNAMODB_PREDICTION_TABLE_HASH_KEY_NAME = System.getenv("PREDICTION_TABLE_HASH_KEY");
    private static final String DYNAMODB_PREDICTION_TABLE_RANGE_KEY_NAME = System.getenv("PREDICTION_TABLE_RANGE_KEY");

    // The item lifespan should be aligned with the forecast horizon
    private static final String DYNAMODB_PREDICTION_TABLE_ITEM_LIFESPAN_IN_DAY_STR = System.getenv("FORECAST_HORIZON_IN_DAYS");
    private static final long DYNAMODB_PREDICTION_TABLE_ITEM_EXPIRATION_TIME = Instant.now()
            .plus(Long.parseLong(DYNAMODB_PREDICTION_TABLE_ITEM_LIFESPAN_IN_DAY_STR), ChronoUnit.DAYS).getEpochSecond();
    private static final DateTimeFormatter PREDICTION_TIMESTAMP_FORMATTER = DateTimeFormat.forPattern("YYYY-MM-dd'T'HH:mm:ss'Z'");
    private static final int REQUIRED_NUMBER_OF_PREDICTION_RESULT_ITEMS_FOR_DERIVING_DATA_FREQUENCY = 2;

    private static final String DYNAMODB_PREDICTION_METADATA_TABLE_NAME = System.getenv("PREDICTION_METADATA_TABLE_NAME");
    private static final String DYNAMODB_PREDICTION_METADATA_HASH_KEY_NAME = System.getenv("PREDICTION_METADATA_TABLE_HASH_KEY");
    private static final String DYNAMODB_PREDICTION_METADATA_ATTRIBUTE_NAME = System.getenv("PREDICTION_METADATA_TABLE_ATTRIBUTE_NAME");

    private static final String PREDICTION_TABLE_CSV_VALUE_SPLITTER = "$";
    @VisibleForTesting
    static final String DYNAMODB_PREDICTION_METADATA_LATEST_PRED_UUID_ATTR_NAME = "LatestPredictionUUID";
    @VisibleForTesting
    static final String DYNAMODB_PREDICTION_METADATA_LATEST_PRED_DATA_FREQ_IN_SEC_ATTR_NAME = "LatestPredictionDataFrequencyInSeconds";

    // An example of prediction file name: target/fej_1571260106456_2019-10-16T21-40-00Z_part0.csv
    private static final String PREDICTION_RESULT_FILE_NAME_REGEX =
                    "^([a-zA-Z0-9_-]+)/" +                              // for matching string like, "target/
                    "([a-zA-Z0-9_-]+)" +                                // for matching forecastExportJob file name string like "fej_1571260106456"
                    "_(\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2}Z)" +   // for matching timestamp like "_2019-10-16T21-40-00Z"
                    "_(part\\d{1}.csv)$";                               // for matching the suffix like "_part0.csv";
    private static final Pattern PREDICTION_RESULT_FILE_NAME_PATTERN = Pattern.compile(PREDICTION_RESULT_FILE_NAME_REGEX);

    /*
     * Refer to: https://docs.oracle.com/javase/7/docs/api/java/util/regex/Matcher.html#group%28int%29
     * group(0) will match the entire group, group(1) is the first group within the parentheses
     */
    private static final int FORECAST_EXPORT_JOB_NAME_INDEX = 2;

    @Inject
    @NonNull
    AmazonS3 s3Client;

    @Inject
    @NonNull
    AmazonDynamoDB ddbClient;

    public LoadDataFromS3ToDynamoDBHandler() {
        DaggerLambdaFunctionsComponent.create().inject(this);
    }

    @Override
    public Void handleRequest(S3Event s3Event, Context context) {
        /*
         * Based on https://forums.aws.amazon.com/thread.jspa?messageID=592264#592264
         * all S3 event notifications have a single event(record) per notification message,
         * which means there will be only 1 record in records list
         */
        S3EventNotificationRecord record = s3Event.getRecords().get(0);
        String srcBucket = record.getS3().getBucket().getName();
        String srcKey = record.getS3().getObject().getKey();

        Matcher predictionResultUuidMatcher = PREDICTION_RESULT_FILE_NAME_PATTERN.matcher(srcKey);
        String forecastExportJobName;
        if (predictionResultUuidMatcher.matches()) {
            forecastExportJobName = predictionResultUuidMatcher.group(FORECAST_EXPORT_JOB_NAME_INDEX);
        } else {
            String errorMsg = String.format("Cannot parse prediction result object key: %s", srcKey);
            throw new RuntimeException(errorMsg);
        }

        S3Object s3Object = s3Client.getObject(new GetObjectRequest(srcBucket, srcKey));
        log.info(String.format("Start processing s3 object: %s, with forecast export job name: %s",
                s3Object.toString(), forecastExportJobName));

        // Read file directly from S3 and converts the records into PredictionResultItem model
        BufferedReader s3ObjectReader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent(), StandardCharsets.UTF_8));
        CsvToBean<PredictionResultItem> csvToBean = new CsvToBeanBuilder<PredictionResultItem>(s3ObjectReader)
                .withType(PredictionResultItem.class)
                .withIgnoreLeadingWhiteSpace(true)
                .build();
        List<PredictionResultItem> predictionResultItems = Lists.newArrayList(csvToBean.iterator());

        int numberOfNewItems = predictionResultItems.size();
        if (numberOfNewItems == 0) {
            throw new RuntimeException(String.format("Prediction result file %s contains no record.", srcKey));
        }
        predictionResultItems.forEach(item ->
        {
            item.setHashKey(String.format("%s%s%s",
                    item.getHashKey(), PREDICTION_TABLE_CSV_VALUE_SPLITTER, forecastExportJobName));
            item.setExpirationTime(DYNAMODB_PREDICTION_TABLE_ITEM_EXPIRATION_TIME);
        });
        log.info(String.format("Finish loading and parsing %d new items from S3.", numberOfNewItems));

        DynamoDBMapper mapper = new DynamoDBMapper(ddbClient,
                new DynamoDBMapperConfig(DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(DYNAMODB_PREDICTION_TABLE_NAME)));
        mapper.batchSave(predictionResultItems);
        log.info("Finish writing to DynamoDB Table.");

        // After populating the PredictionResultItem table, we get the first 2 items for any hashKey
        // and calculate the data frequency by comparing the rangeKey(sortKey)
        Condition hashKeyCondition = new Condition()
                .withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue(predictionResultItems.get(0).getHashKey()));
        Map<String, Condition> keyConditions = new HashMap<>();
        keyConditions.put(DYNAMODB_PREDICTION_TABLE_HASH_KEY_NAME, hashKeyCondition);
        QueryRequest queryRequest = new QueryRequest()
                .withTableName(DYNAMODB_PREDICTION_TABLE_NAME)
                .withKeyConditions(keyConditions)
                .withConsistentRead(true)
                .withScanIndexForward(true) /* ascending order for the range key*/
                .withLimit(REQUIRED_NUMBER_OF_PREDICTION_RESULT_ITEMS_FOR_DERIVING_DATA_FREQUENCY); /* get the first 2 items */
        QueryResult queryResult = ddbClient.query(queryRequest);
        long predictionDataFreqInSecs = derivePredDataFreqFromConsecutiveItems(queryResult.getItems());

        // Write latestPredictionUUID and latestPredictionDataFrequency to PredictionMetadata table IN A SINGLE TRANSACTION
        Map<String, AttributeValue> latestPredictionUUIDItem = new HashMap<>();
        latestPredictionUUIDItem.put(DYNAMODB_PREDICTION_METADATA_HASH_KEY_NAME,
                new AttributeValue(DYNAMODB_PREDICTION_METADATA_LATEST_PRED_UUID_ATTR_NAME));
        latestPredictionUUIDItem.put(DYNAMODB_PREDICTION_METADATA_ATTRIBUTE_NAME, new AttributeValue(forecastExportJobName));
        Put latestPredictionUUIDItemWrite = new Put()
                .withTableName(DYNAMODB_PREDICTION_METADATA_TABLE_NAME)
                .withItem(latestPredictionUUIDItem);

        Map<String, AttributeValue> latestPredictionDataFrequencyItem = new HashMap<>();
        latestPredictionDataFrequencyItem.put(DYNAMODB_PREDICTION_METADATA_HASH_KEY_NAME,
                new AttributeValue(DYNAMODB_PREDICTION_METADATA_LATEST_PRED_DATA_FREQ_IN_SEC_ATTR_NAME));
        latestPredictionDataFrequencyItem.put(DYNAMODB_PREDICTION_METADATA_ATTRIBUTE_NAME,
                new AttributeValue(String.valueOf(predictionDataFreqInSecs)));
        Put latestPredictionDataFrequencyWrite = new Put()
                .withTableName(DYNAMODB_PREDICTION_METADATA_TABLE_NAME)
                .withItem(latestPredictionDataFrequencyItem);

        Collection<TransactWriteItem> transactWrites = Arrays.asList(
                new TransactWriteItem().withPut(latestPredictionUUIDItemWrite),
                new TransactWriteItem().withPut(latestPredictionDataFrequencyWrite)
        );
        TransactWriteItemsRequest writeItemsRequest = new TransactWriteItemsRequest()
                .withTransactItems(transactWrites);

        ddbClient.transactWriteItems(writeItemsRequest);
        log.info("Finish updating new metadata items for the latest prediction");

        // Not bother to close all the file descriptors as lambda function will cleanup them after termination

        return null;
    }

    /**
     * Derive the data frequency by calculating the diff on the rangeKey(timestamp) for the top two items.
     * We can make the assumption that one prediction result file can only have one data frequency.
     * @return The data frequency(window size) in seconds
     */
    private long derivePredDataFreqFromConsecutiveItems(List<Map<String, AttributeValue>> items) {
        if (items == null || items.size() < REQUIRED_NUMBER_OF_PREDICTION_RESULT_ITEMS_FOR_DERIVING_DATA_FREQUENCY) {
            throw new RuntimeException(String.format("Passed in items contains %d item, which is less than 2.",
                    items == null ? 0 : items.size()));
        }

        Map<String, AttributeValue> firstItem = items.get(0);
        Map<String, AttributeValue> secondItem = items.get(1);
        String firstTsStr = firstItem.get(DYNAMODB_PREDICTION_TABLE_RANGE_KEY_NAME).getS();
        String secondTsStr = secondItem.get(DYNAMODB_PREDICTION_TABLE_RANGE_KEY_NAME).getS();
        long dataFreqInSeconds = Math.abs(new Duration(PREDICTION_TIMESTAMP_FORMATTER.parseDateTime(firstTsStr),
                PREDICTION_TIMESTAMP_FORMATTER.parseDateTime(secondTsStr)).getStandardSeconds());

        if (dataFreqInSeconds == 0) {
            throw new RuntimeException(String.format("dataFreqInSeconds [%d] derived from firstItem [%s] and secondItem [%s] is 0",
                    dataFreqInSeconds, firstItem.toString(), secondItem.toString()));
        }
        return dataFreqInSeconds;
    }
}
