package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.services.forecast.AmazonForecast;
import com.amazonaws.services.forecast.model.DatasetSummary;
import com.amazonaws.services.forecast.model.ForecastSummary;
import com.amazonaws.services.forecast.model.ListDatasetsRequest;
import com.amazonaws.services.forecast.model.ListDatasetsResult;
import com.amazonaws.services.forecast.model.ListForecastsRequest;
import com.amazonaws.services.forecast.model.ListForecastsResult;
import com.amazonaws.services.forecast.model.ListPredictorsRequest;
import com.amazonaws.services.forecast.model.ListPredictorsResult;
import com.amazonaws.services.forecast.model.PredictorSummary;
import com.google.common.collect.ImmutableMap;
import lombok.NonNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;


public final class PredictionGenerationUtils {

    // Private Constructor will prevent the instantiation of this class directly
    private PredictionGenerationUtils() {}

    private static final Duration ONE_MIN_DURATION = Duration.ofMinutes(1);
    private static final Duration FIVE_MIN_DURATION = Duration.ofMinutes(5);
    private static final Duration TEN_MIN_DURATION = Duration.ofMinutes(10);
    private static final Duration FIFTEEN_MIN_DURATION = Duration.ofMinutes(15);
    private static final Duration THIRTY_MIN_DURATION = Duration.ofMinutes(30);
    private static final Duration ONE_HOUR_DURATION = Duration.ofHours(1);
    private static final Duration ONE_DAY_DURATION = Duration.ofDays(1);
    private static final Duration ONE_WEEK_DURATION = Duration.ofDays(7);
    private static final Duration ONE_MONTH_DURATION = Duration.ofDays(30);

    // Refer to: https://docs.aws.amazon.com/forecast/latest/dg/API_CreateDataset.html#forecast-CreateDataset-request-DataFrequency
    static final String ONE_MIN_DATA_FREQUENCY_STRING = "1min";
    static final String FIVE_MIN_DATA_FREQUENCY_STRING = "5min";
    static final String TEN_MIN_DATA_FREQUENCY_STRING = "10min";
    static final String FIFTEEN_MIN_DATA_FREQUENCY_STRING = "15min";
    static final String THIRTY_MIN_DATA_FREQUENCY_STRING = "30min";
    static final String ONE_HOUR_DATA_FREQUENCY_STRING = "H";
    static final String ONE_DAY_DATA_FREQUENCY_STRING = "D";
    static final String ONE_WEEK_DATA_FREQUENCY_STRING = "W";
    static final String ONE_MONTH_DATA_FREQUENCY_STRING = "M";
    static final String ONE_YEAR_DATA_FREQUENCY_STRING = "Y";
    static final Map<String, Integer> DATA_FREQUENCY_SECONDS_MAPPING =
            ImmutableMap.<String, Integer>builder()
                    .put(FIVE_MIN_DATA_FREQUENCY_STRING, 5*60)
                    .put(TEN_MIN_DATA_FREQUENCY_STRING, 10*60)
                    .put(FIFTEEN_MIN_DATA_FREQUENCY_STRING, 15*60)
                    .put(THIRTY_MIN_DATA_FREQUENCY_STRING, 30*60)
                    .put(ONE_HOUR_DATA_FREQUENCY_STRING, 60*60)
                    .put(ONE_DAY_DATA_FREQUENCY_STRING, 60*60*24)
                    .put(ONE_WEEK_DATA_FREQUENCY_STRING, 60*60*24*7)
                    .put(ONE_MONTH_DATA_FREQUENCY_STRING, 60*60*24*30)
                    .put(ONE_YEAR_DATA_FREQUENCY_STRING, 60*60*24*365)
                    .build();

    static final String ARN_COMPONENT_SPLITTER = ":";
    static final String FORECAST_SERVICE_NAME = "forecast";
    static final String DATASET_NAME_PREFIX = "ds_";
    static final String DATASET_GROUP_NAME_PREFIX = "dsg_";
    static final String DATASET_IMPORT_JOB_NAME_PREFIX = "dsij_";
    static final String PREDICTOR_NAME_PREFIX = "p_";
    static final String FORECAST_NAME_PREFIX = "f_";
    static final String FORECAST_EXPORT_JOB_NAME_PREFIX = "fej_";

    static final String FORECAST_RESOURCE_ARN_PREFIX_KEY = "ForecastResourceArnPrefixKey";
    static final String DATASET_NAME_KEY = "DatasetName";
    static final String DATASET_ARN_KEY = "DatasetArn";
    static final String DATASET_GROUP_NAME_KEY = "DatasetGroupName";
    static final String DATASET_GROUP_ARN_KEY = "DatasetGroupArn";
    static final String DATASET_IMPORT_JOB_NAME_KEY = "DatasetImportJobName";
    static final String DATASET_IMPORT_JOB_ARN_KEY = "DatasetImportJobArn";
    static final String PREDICTOR_NAME_KEY = "PredictorName";
    static final String PREDICTOR_ARN_KEY = "PredictorArn";
    static final String FORECAST_NAME_KEY = "ForecastName";
    static final String FORECAST_ARN_KEY = "ForecastArn";
    static final String FORECAST_EXPORT_JOB_NAME_KEY = "ForecastExportJobName";
    static final String FORECAST_EXPORT_JOB_ARN_KEY = "ForecastExportJobArn";
    static final String DATA_FREQUENCY_KEY = "DataFrequency";

    static final String RESOURCE_ACTIVE_STATUS = "ACTIVE";
    static final String RESOURCE_FAILED_STATUS = "FAILED";

    static DatasetSummary getLatestDataset(final AmazonForecast forecastClient) {
        List<DatasetSummary> existingDatasets = listDatasets(forecastClient);
        return existingDatasets
                .stream()
                .max(Comparator.comparing(DatasetSummary::getCreationTime)).orElse(null);
    }

    static List<DatasetSummary> listDatasets(final AmazonForecast forecastClient) {
        List<DatasetSummary> existingDatasets = new ArrayList<>();
        String nextToken = null;
        do {
            ListDatasetsRequest listDatasetsRequest = new ListDatasetsRequest();
            if (nextToken != null) {
                listDatasetsRequest.setNextToken(nextToken);
            }
            ListDatasetsResult listDatasetsResult = forecastClient.listDatasets(listDatasetsRequest);
            existingDatasets.addAll(listDatasetsResult.getDatasets());
            nextToken = listDatasetsResult.getNextToken();
        } while (nextToken != null);

        return existingDatasets;
    }

    static PredictorSummary getLatestPredictor(final AmazonForecast forecastClient) {
        List<PredictorSummary> existingPredictors = listPredictors(forecastClient);
        return existingPredictors.stream().max(Comparator.comparing(PredictorSummary::getCreationTime)).orElse(null);
    }

    /**
     * TODO: following two methods have very similar strcuture, which is keeping call list call until the next token is null.
     * We should parameter them into one method.
     */
    static List<PredictorSummary> listPredictors(AmazonForecast forecastClient) {
        List<PredictorSummary> existingPredictors = new ArrayList<>();
        String nextToken = null;
        do {
            ListPredictorsRequest listPredictorsRequest = new ListPredictorsRequest();
            if (nextToken != null) {
                listPredictorsRequest.setNextToken(nextToken);
            }
            ListPredictorsResult listPredictorsResult = forecastClient.listPredictors(listPredictorsRequest);

            existingPredictors.addAll(listPredictorsResult.getPredictors());
            nextToken = listPredictorsResult.getNextToken();
        } while (nextToken != null);

        return existingPredictors;
    }

    static List<ForecastSummary> listForecasts(AmazonForecast forecastClient) {
        List<ForecastSummary> existingForecasts = new ArrayList<>();
        String nextToken = null;
        do {
            ListForecastsRequest listForecastsRequest = new ListForecastsRequest();
            if (nextToken != null) {
                listForecastsRequest.setNextToken(nextToken);
            }
            ListForecastsResult listForecastsResult = forecastClient.listForecasts(listForecastsRequest);

            existingForecasts.addAll(listForecastsResult.getForecasts());
            nextToken = listForecastsResult.getNextToken();
        } while (nextToken != null);

        return existingForecasts;
    }

    /**
     * Convert lambda function arn, e.g.:
     *  arn:aws:lambda:us-east-1:443299619838:function:CreateDataset
     * to forecast resource arn prefix, e.g.:
     *  arn:aws:forecast:us-west-2:443299619838
     */
    static String deriveForecastResourceArnPrefixFromLambdaFunctionArn(@NonNull final String functionArn) {
        String[] functionArnComponents = functionArn.split(ARN_COMPONENT_SPLITTER);
        StringJoiner forecastResourceArnPrefix = new StringJoiner(ARN_COMPONENT_SPLITTER, "", ARN_COMPONENT_SPLITTER);
        forecastResourceArnPrefix
                .add(functionArnComponents[0])  // arn
                .add(functionArnComponents[1])  // partition: "aws", or "aws-cn"
                .add(FORECAST_SERVICE_NAME)     // forecast
                .add(functionArnComponents[3])  // region: "us-west-2"
                .add(functionArnComponents[4]); // accountId: "0123456789"
        return forecastResourceArnPrefix.toString();
    }

    static String getForecastDataFrequencyStr(final Duration dataFrequencyDuration) {

        /*
         * Refer to: https://docs.aws.amazon.com/forecast/latest/dg/API_CreateDataset.html#forecast-CreateDataset-request-DataFrequency,
         * Valid intervals are Y (Year), M (Month), W (Week), D (Day), H (Hour), 30min (30 minutes),
         * 15min (15 minutes), 10min (10 minutes), 5min (5 minutes), and 1min (1 minute).
         */
        if (dataFrequencyDuration.compareTo(ONE_MIN_DURATION) <= 0) { // below or equal 1 min
            return ONE_MIN_DATA_FREQUENCY_STRING;
        }
        if (dataFrequencyDuration.compareTo(FIVE_MIN_DURATION) <= 0) { // below or equal 5 mins
            return FIVE_MIN_DATA_FREQUENCY_STRING;
        }
        if (dataFrequencyDuration.compareTo(TEN_MIN_DURATION) <= 0) { // below or equal 10 mins
            return TEN_MIN_DATA_FREQUENCY_STRING;
        }
        if (dataFrequencyDuration.compareTo(FIFTEEN_MIN_DURATION) <= 0) { // below or equal 15 mins
            return FIFTEEN_MIN_DATA_FREQUENCY_STRING;
        }
        if (dataFrequencyDuration.compareTo(THIRTY_MIN_DURATION) <= 0) { // below or equal 30 mins
            return THIRTY_MIN_DATA_FREQUENCY_STRING;
        }
        if (dataFrequencyDuration.compareTo(ONE_HOUR_DURATION) <= 0) { // below or equal 1 hour
            return ONE_HOUR_DATA_FREQUENCY_STRING;
        }
        if (dataFrequencyDuration.compareTo(ONE_DAY_DURATION) <= 0) { // below or equal 1 day
            return ONE_DAY_DATA_FREQUENCY_STRING;
        }
        if (dataFrequencyDuration.compareTo(ONE_WEEK_DURATION) <= 0) { // below or equal 1 week
            return ONE_WEEK_DATA_FREQUENCY_STRING;
        }
        if (dataFrequencyDuration.compareTo(ONE_MONTH_DURATION) <= 0) { // below or equal 1 month
            return ONE_MONTH_DATA_FREQUENCY_STRING;
        }
        return ONE_YEAR_DATA_FREQUENCY_STRING;
    }
}
