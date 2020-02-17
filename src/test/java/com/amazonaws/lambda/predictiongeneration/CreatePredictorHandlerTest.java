package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.lambda.predictiongeneration.exception.ResourceSetupFailureException;
import com.amazonaws.lambda.predictiongeneration.exception.ResourceSetupInProgressException;
import com.amazonaws.services.forecast.model.CreatePredictorRequest;
import com.amazonaws.services.forecast.model.DescribePredictorRequest;
import com.amazonaws.services.forecast.model.DescribePredictorResult;
import com.amazonaws.services.forecast.model.FeaturizationConfig;
import com.amazonaws.services.forecast.model.InputDataConfig;
import com.amazonaws.services.forecast.model.ResourceNotFoundException;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static com.amazonaws.lambda.predictiongeneration.CreatePredictorHandler.SECONDS_IN_A_DAY;
import static com.amazonaws.lambda.predictiongeneration.GenerateForecastResourcesIdsHandler.buildResourceIdMap;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_GROUP_NAME_PREFIX;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATA_FREQUENCY_SECONDS_MAPPING;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.PREDICTOR_NAME_PREFIX;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.RESOURCE_ACTIVE_STATUS;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.RESOURCE_FAILED_STATUS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CreatePredictorHandlerTest extends BaseTest {

    private static final int TEST_FORECAST_HORIZON_IN_DAYS = 3;
    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    CreatePredictorHandler handler;

    @BeforeEach
    public void setup() {
        environmentVariables.set("FORECAST_HORIZON_IN_DAYS", String.valueOf(TEST_FORECAST_HORIZON_IN_DAYS));
        handler = new CreatePredictorHandler(mockForecastClient);
    }

    @Test
    public void testProcess_withActiveStatus() {
        DescribePredictorResult dummyDescribePredictorResult = new DescribePredictorResult().withStatus(RESOURCE_ACTIVE_STATUS);
        when(mockForecastClient.describePredictor(any(DescribePredictorRequest.class))).thenReturn(dummyDescribePredictorResult);

        handler.process(buildResourceIdMap(System.currentTimeMillis(), TEST_FORECAST_RESOURCE_ARN, DEFAULT_DATA_FREQUENCY_VALUE));

        verify(mockForecastClient, times(1)).describePredictor(any(DescribePredictorRequest.class));
        verify(mockForecastClient, never()).createPredictor(any(CreatePredictorRequest.class));
    }

    @Test
    public void testProcess_withFailedStatus() {
        DescribePredictorResult dummyDescribePredictorResult = new DescribePredictorResult().withStatus(RESOURCE_FAILED_STATUS);
        when(mockForecastClient.describePredictor(any(DescribePredictorRequest.class))).thenReturn(dummyDescribePredictorResult);

        assertThrows(ResourceSetupFailureException.class,
                () -> handler.process(buildResourceIdMap(System.currentTimeMillis(), TEST_FORECAST_RESOURCE_ARN, DEFAULT_DATA_FREQUENCY_VALUE)));

        verify(mockForecastClient, times(1)).describePredictor(any(DescribePredictorRequest.class));
        verify(mockForecastClient, never()).createPredictor(any(CreatePredictorRequest.class));
    }

    @Test
    public void testProcess_withCreateNewPredictorThenInCreating() {
        DescribePredictorResult dummyDescribePredictorResult = new DescribePredictorResult().withStatus(TEST_RESOURCE_CREATING_STATUS);
        when(mockForecastClient.describePredictor(any(DescribePredictorRequest.class)))
                .thenAnswer(new Answer<DescribePredictorResult>() {
                    private int count = 0;

                    @Override
                    public DescribePredictorResult answer(InvocationOnMock invocation) {
                        switch (count++) {
                            case 0:
                                throw new ResourceNotFoundException("cannot find given predictor");
                            case 1:
                                return dummyDescribePredictorResult;
                            default:
                                throw new IllegalArgumentException();
                        }
                    }
                });
        long currentTimeMillis = System.currentTimeMillis();
        CreatePredictorRequest expectedCreatePredictorRequest = new CreatePredictorRequest()
                .withForecastHorizon(TEST_FORECAST_HORIZON_IN_DAYS * SECONDS_IN_A_DAY / DATA_FREQUENCY_SECONDS_MAPPING.get(DEFAULT_DATA_FREQUENCY_VALUE))
                .withFeaturizationConfig(new FeaturizationConfig().withForecastFrequency(DEFAULT_DATA_FREQUENCY_VALUE))
                .withInputDataConfig(new InputDataConfig().withDatasetGroupArn(TEST_FORECAST_RESOURCE_ARN + "dataset-group/" + DATASET_GROUP_NAME_PREFIX + currentTimeMillis))
                .withPredictorName(PREDICTOR_NAME_PREFIX + currentTimeMillis)
                .withPerformAutoML(true);

        assertThrows(ResourceSetupInProgressException.class,
                () -> handler.process(buildResourceIdMap(currentTimeMillis, TEST_FORECAST_RESOURCE_ARN, DEFAULT_DATA_FREQUENCY_VALUE)));

        verify(mockForecastClient, times(2)).describePredictor(any(DescribePredictorRequest.class));
        verify(mockForecastClient, times(1)).createPredictor(eq(expectedCreatePredictorRequest));
    }

    @Test
    public void testProcess_withCreateNewPredictorThenInActive() {
        DescribePredictorResult dummyDescribePredictorResult = new DescribePredictorResult().withStatus(RESOURCE_ACTIVE_STATUS);
        when(mockForecastClient.describePredictor(any(DescribePredictorRequest.class)))
                .thenAnswer(new Answer<DescribePredictorResult>() {
                    private int count = 0;

                    @Override
                    public DescribePredictorResult answer(InvocationOnMock invocation) {
                        switch (count++) {
                            case 0:
                                throw new ResourceNotFoundException("cannot find given predictor");
                            case 1:
                                return dummyDescribePredictorResult;
                            default:
                                throw new IllegalArgumentException();
                        }
                    }
                });

        handler.process(buildResourceIdMap(System.currentTimeMillis(), TEST_FORECAST_RESOURCE_ARN, DEFAULT_DATA_FREQUENCY_VALUE));

        verify(mockForecastClient, times(2)).describePredictor(any(DescribePredictorRequest.class));
        verify(mockForecastClient, times(1)).createPredictor(any(CreatePredictorRequest.class));
    }
}
