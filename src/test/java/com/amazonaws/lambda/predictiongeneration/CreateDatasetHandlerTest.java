package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.lambda.predictiongeneration.exception.ResourceSetupFailureException;
import com.amazonaws.lambda.predictiongeneration.exception.ResourceSetupInProgressException;
import com.amazonaws.services.forecast.model.CreateDatasetRequest;
import com.amazonaws.services.forecast.model.DescribeDatasetRequest;
import com.amazonaws.services.forecast.model.DescribeDatasetResult;
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

public class CreateDatasetHandlerTest extends BaseTest {

    CreateDatasetHandler handler;

    @BeforeEach
    public void setup() {
        handler = new CreateDatasetHandler(mockForecastClient);
    }

    @Test
    public void testProcess_withActiveStatus() {
        DescribeDatasetResult dummyDescribeDatasetResult = new DescribeDatasetResult().withStatus(RESOURCE_ACTIVE_STATUS);
        when(mockForecastClient.describeDataset(any(DescribeDatasetRequest.class))).thenReturn(dummyDescribeDatasetResult);

        handler.process(buildResourceIdMap(System.currentTimeMillis(), TEST_FORECAST_RESOURCE_ARN, DEFAULT_DATA_FREQUENCY_VALUE));

        verify(mockForecastClient, times(1)).describeDataset(any(DescribeDatasetRequest.class));
        verify(mockForecastClient, never()).createDataset(any(CreateDatasetRequest.class));
    }

    @Test
    public void testProcess_withFailedStatus() {
        DescribeDatasetResult dummyDescribeDatasetResult = new DescribeDatasetResult().withStatus(RESOURCE_FAILED_STATUS);
        when(mockForecastClient.describeDataset(any(DescribeDatasetRequest.class))).thenReturn(dummyDescribeDatasetResult);

        assertThrows(ResourceSetupFailureException.class,
                () -> handler.process(buildResourceIdMap(System.currentTimeMillis(), TEST_FORECAST_RESOURCE_ARN, DEFAULT_DATA_FREQUENCY_VALUE)));

        verify(mockForecastClient, times(1)).describeDataset(any(DescribeDatasetRequest.class));
        verify(mockForecastClient, never()).createDataset(any(CreateDatasetRequest.class));
    }

    @Test
    public void testProcess_withCreateNewDatasetThenInCreating() {
        DescribeDatasetResult dummyDescribeDatasetResult = new DescribeDatasetResult().withStatus(TEST_RESOURCE_CREATING_STATUS);
        when(mockForecastClient.describeDataset(any(DescribeDatasetRequest.class)))
                .thenAnswer(new Answer<DescribeDatasetResult>() {
                    private int count = 0;

                    @Override
                    public DescribeDatasetResult answer(InvocationOnMock invocation) {
                        switch (count++) {
                            case 0:
                                throw new ResourceNotFoundException("cannot find given dataset");
                            case 1:
                                return dummyDescribeDatasetResult;
                            default:
                                throw new IllegalArgumentException();
                        }
                    }
                });

        assertThrows(ResourceSetupInProgressException.class,
                () -> handler.process(buildResourceIdMap(System.currentTimeMillis(), TEST_FORECAST_RESOURCE_ARN, DEFAULT_DATA_FREQUENCY_VALUE)));

        verify(mockForecastClient, times(2)).describeDataset(any(DescribeDatasetRequest.class));
        verify(mockForecastClient, times(1)).createDataset(any(CreateDatasetRequest.class));
    }

    @Test
    public void testProcess_withCreateNewDatasetThenInActive() {
        DescribeDatasetResult dummyDescribeDatasetResult = new DescribeDatasetResult().withStatus(RESOURCE_ACTIVE_STATUS);
        when(mockForecastClient.describeDataset(any(DescribeDatasetRequest.class)))
                .thenAnswer(new Answer<DescribeDatasetResult>() {
                    private int count = 0;

                    @Override
                    public DescribeDatasetResult answer(InvocationOnMock invocation) {
                        switch (count++) {
                            case 0:
                                throw new ResourceNotFoundException("cannot find given dataset");
                            case 1:
                                return dummyDescribeDatasetResult;
                            default:
                                throw new IllegalArgumentException();
                        }
                    }
                });

        handler.process(buildResourceIdMap(System.currentTimeMillis(), TEST_FORECAST_RESOURCE_ARN, DEFAULT_DATA_FREQUENCY_VALUE));

        verify(mockForecastClient, times(2)).describeDataset(any(DescribeDatasetRequest.class));
        verify(mockForecastClient, times(1)).createDataset(any(CreateDatasetRequest.class));
    }
}
