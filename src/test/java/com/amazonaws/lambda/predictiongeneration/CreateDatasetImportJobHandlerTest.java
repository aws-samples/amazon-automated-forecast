package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.lambda.predictiongeneration.exception.ResourceSetupFailureException;
import com.amazonaws.lambda.predictiongeneration.exception.ResourceSetupInProgressException;
import com.amazonaws.services.forecast.model.CreateDatasetImportJobRequest;
import com.amazonaws.services.forecast.model.DescribeDatasetImportJobRequest;
import com.amazonaws.services.forecast.model.DescribeDatasetImportJobResult;
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

public class CreateDatasetImportJobHandlerTest extends BaseTest {

    CreateDatasetImportJobHandler handler;

    @BeforeEach
    public void setup() {
        handler = new CreateDatasetImportJobHandler(mockForecastClient);
    }

    @Test
    public void testProcess_withActiveStatus() {
        DescribeDatasetImportJobResult dummyDescribeDatasetImportJobResult = new DescribeDatasetImportJobResult() .withStatus(RESOURCE_ACTIVE_STATUS);
        when(mockForecastClient.describeDatasetImportJob(any(DescribeDatasetImportJobRequest.class))).thenReturn(dummyDescribeDatasetImportJobResult);

        handler.process(buildResourceIdMap(System.currentTimeMillis(), TEST_FORECAST_RESOURCE_ARN, DEFAULT_DATA_FREQUENCY_VALUE));

        verify(mockForecastClient, times(1)).describeDatasetImportJob(any(DescribeDatasetImportJobRequest.class));
        verify(mockForecastClient, never()).createDatasetImportJob(any(CreateDatasetImportJobRequest.class));
    }

    @Test
    public void testProcess_withFailedStatus() {
        DescribeDatasetImportJobResult dummyDescribeDatasetImportJobResult = new DescribeDatasetImportJobResult().withStatus(RESOURCE_FAILED_STATUS);
        when(mockForecastClient.describeDatasetImportJob(any(DescribeDatasetImportJobRequest.class))).thenReturn(dummyDescribeDatasetImportJobResult);

        assertThrows(ResourceSetupFailureException.class,
                () -> handler.process(buildResourceIdMap(System.currentTimeMillis(), TEST_FORECAST_RESOURCE_ARN, DEFAULT_DATA_FREQUENCY_VALUE)));

        verify(mockForecastClient, times(1)).describeDatasetImportJob(any(DescribeDatasetImportJobRequest.class));
        verify(mockForecastClient, never()).createDatasetImportJob(any(CreateDatasetImportJobRequest.class));
    }

    @Test
    public void testProcess_withCreateNewDatasetImportJobThenInCreating() {
        DescribeDatasetImportJobResult dummyDescribeDatasetImportJobResult = new DescribeDatasetImportJobResult().withStatus(TEST_RESOURCE_CREATING_STATUS);
        when(mockForecastClient.describeDatasetImportJob(any(DescribeDatasetImportJobRequest.class)))
                .thenAnswer(new Answer<DescribeDatasetImportJobResult>() {
                    private int count = 0;

                    @Override
                    public DescribeDatasetImportJobResult answer(InvocationOnMock invocation) {
                        switch (count++) {
                            case 0:
                                throw new ResourceNotFoundException("cannot find given dataset import job");
                            case 1:
                                return dummyDescribeDatasetImportJobResult;
                            default:
                                throw new IllegalArgumentException();
                        }
                    }
                });

        assertThrows(ResourceSetupInProgressException.class,
                () -> handler.process(buildResourceIdMap(System.currentTimeMillis(), TEST_FORECAST_RESOURCE_ARN, DEFAULT_DATA_FREQUENCY_VALUE)));

        verify(mockForecastClient, times(2)).describeDatasetImportJob(any(DescribeDatasetImportJobRequest.class));
        verify(mockForecastClient, times(1)).createDatasetImportJob(any(CreateDatasetImportJobRequest.class));
    }

    @Test
    public void testProcess_withCreateNewDatasetImportJobThenInActive() {
        DescribeDatasetImportJobResult dummyDescribeDatasetImportJobResult = new DescribeDatasetImportJobResult().withStatus(RESOURCE_ACTIVE_STATUS);
        when(mockForecastClient.describeDatasetImportJob(any(DescribeDatasetImportJobRequest.class)))
                .thenAnswer(new Answer<DescribeDatasetImportJobResult>() {
                    private int count = 0;

                    @Override
                    public DescribeDatasetImportJobResult answer(InvocationOnMock invocation) {
                        switch (count++) {
                            case 0:
                                throw new ResourceNotFoundException("cannot find given dataset import job");
                            case 1:
                                return dummyDescribeDatasetImportJobResult;
                            default:
                                throw new IllegalArgumentException();
                        }
                    }
                });

        handler.process(buildResourceIdMap(System.currentTimeMillis(), TEST_FORECAST_RESOURCE_ARN, DEFAULT_DATA_FREQUENCY_VALUE));

        verify(mockForecastClient, times(2)).describeDatasetImportJob(any(DescribeDatasetImportJobRequest.class));
        verify(mockForecastClient, times(1)).createDatasetImportJob(any(CreateDatasetImportJobRequest.class));
    }
}
