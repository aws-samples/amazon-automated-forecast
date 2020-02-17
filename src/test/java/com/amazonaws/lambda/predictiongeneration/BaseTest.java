package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.services.forecast.AmazonForecast;
import org.junit.jupiter.api.BeforeEach;

import java.util.Map;

import static com.amazonaws.lambda.predictiongeneration.GenerateForecastResourcesIdsHandler.buildResourceIdMap;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.deriveForecastResourceArnPrefixFromLambdaFunctionArn;
import static org.mockito.Mockito.mock;

public class BaseTest {

    protected static final String TEST_FUNCTION_ARN = "arn:aws:lambda:us-east-1:012345678901:function:Dummy";
    protected static final String TEST_FORECAST_RESOURCE_ARN
            = deriveForecastResourceArnPrefixFromLambdaFunctionArn(TEST_FUNCTION_ARN);
    protected static final String TEST_RESOURCE_CREATING_STATUS = "CREATING";

    static final String DEFAULT_DATA_FREQUENCY_VALUE = "30min";

    protected AmazonForecast mockForecastClient;
    protected Map<String, String> testResourceIdMap;

    @BeforeEach
    public void baseSetup() {
        mockForecastClient = mock(AmazonForecast.class);
        testResourceIdMap = buildResourceIdMap(System.currentTimeMillis(), TEST_FORECAST_RESOURCE_ARN, DEFAULT_DATA_FREQUENCY_VALUE);
    }
}
