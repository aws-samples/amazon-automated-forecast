package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.lambda.predictiongeneration.exception.ResourceSetupFailureException;
import com.amazonaws.lambda.predictiongeneration.exception.ResourceSetupInProgressException;
import com.amazonaws.services.forecast.model.CreateForecastRequest;
import com.amazonaws.services.forecast.model.DescribeForecastRequest;
import com.amazonaws.services.forecast.model.DescribeForecastResult;
import com.amazonaws.services.forecast.model.ResourceNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static com.amazonaws.lambda.predictiongeneration.GenerateForecastResourcesIdsHandler.buildResourceIdMap;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.RESOURCE_ACTIVE_STATUS;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.RESOURCE_FAILED_STATUS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CreateForecastHandlerTest extends BaseTest {

    CreateForecastHandler handler;

    @BeforeEach
    public void setup() {
        handler = new CreateForecastHandler(mockForecastClient);
    }

    @Test
    public void testProcess_withActiveStatus() {
        DescribeForecastResult dummyDescribeForecastResult = new DescribeForecastResult().withStatus(RESOURCE_ACTIVE_STATUS);
        when(mockForecastClient.describeForecast(any(DescribeForecastRequest.class))).thenReturn(dummyDescribeForecastResult);

        handler.process(buildResourceIdMap(System.currentTimeMillis(), TEST_FORECAST_RESOURCE_ARN, DEFAULT_DATA_FREQUENCY_VALUE));

        verify(mockForecastClient, times(1)).describeForecast(any(DescribeForecastRequest.class));
        verify(mockForecastClient, never()).createForecast(any(CreateForecastRequest.class));
    }

    @Test
    public void testProcess_withFailedStatus() {
        DescribeForecastResult dummyDescribeForecastResult = new DescribeForecastResult().withStatus(RESOURCE_FAILED_STATUS);
        when(mockForecastClient.describeForecast(any(DescribeForecastRequest.class))).thenReturn(dummyDescribeForecastResult);

        assertThrows(ResourceSetupFailureException.class,
                () -> handler.process(buildResourceIdMap(System.currentTimeMillis(), TEST_FORECAST_RESOURCE_ARN, DEFAULT_DATA_FREQUENCY_VALUE)));

        verify(mockForecastClient, times(1)).describeForecast(any(DescribeForecastRequest.class));
        verify(mockForecastClient, never()).createForecast(any(CreateForecastRequest.class));
    }

    @Test
    public void testProcess_withCreateNewForecastThenInCreating() {
        DescribeForecastResult dummyDescribeForecastResult = new DescribeForecastResult().withStatus(TEST_RESOURCE_CREATING_STATUS);
        when(mockForecastClient.describeForecast(any(DescribeForecastRequest.class)))
                .thenAnswer(new Answer<DescribeForecastResult>() {
                    private int count = 0;

                    @Override
                    public DescribeForecastResult answer(InvocationOnMock invocation) {
                        switch (count++) {
                            case 0:
                                throw new ResourceNotFoundException("cannot find given forecast");
                            case 1:
                                return dummyDescribeForecastResult;
                            default:
                                throw new IllegalArgumentException();
                        }
                    }
                });

        assertThrows(ResourceSetupInProgressException.class,
                () -> handler.process(buildResourceIdMap(System.currentTimeMillis(), TEST_FORECAST_RESOURCE_ARN, DEFAULT_DATA_FREQUENCY_VALUE)));

        verify(mockForecastClient, times(2)).describeForecast(any(DescribeForecastRequest.class));
        verify(mockForecastClient, times(1)).createForecast(any(CreateForecastRequest.class));
    }

    @Test
    public void testProcess_withCreateNewForecastThenInActive() {
        DescribeForecastResult dummyDescribeForecastResult = new DescribeForecastResult().withStatus(RESOURCE_ACTIVE_STATUS);
        when(mockForecastClient.describeForecast(any(DescribeForecastRequest.class)))
                .thenAnswer(new Answer<DescribeForecastResult>() {
                    private int count = 0;

                    @Override
                    public DescribeForecastResult answer(InvocationOnMock invocation) {
                        switch (count++) {
                            case 0:
                                throw new ResourceNotFoundException("cannot find given forecast");
                            case 1:
                                return dummyDescribeForecastResult;
                            default:
                                throw new IllegalArgumentException();
                        }
                    }
                });

        handler.process(buildResourceIdMap(System.currentTimeMillis(), TEST_FORECAST_RESOURCE_ARN, DEFAULT_DATA_FREQUENCY_VALUE));

        verify(mockForecastClient, times(2)).describeForecast(any(DescribeForecastRequest.class));
        verify(mockForecastClient, times(1)).createForecast(any(CreateForecastRequest.class));
    }
}
