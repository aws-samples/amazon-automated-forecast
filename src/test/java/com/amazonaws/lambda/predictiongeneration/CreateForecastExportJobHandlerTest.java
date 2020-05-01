package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.lambda.predictiongeneration.exception.ResourceSetupFailureException;
import com.amazonaws.lambda.predictiongeneration.exception.ResourceSetupInProgressException;
import com.amazonaws.services.forecast.model.CreateForecastExportJobRequest;
import com.amazonaws.services.forecast.model.DescribeForecastExportJobRequest;
import com.amazonaws.services.forecast.model.DescribeForecastExportJobResult;
import com.amazonaws.services.forecast.model.ResourceNotFoundException;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
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

public class CreateForecastExportJobHandlerTest extends BaseTest {

    private static final String TEST_FORECAST_EXPORT_RESULT_ROLE_ARN = "testExportResultRoleArn";
    private static final String TEST_PREDICTION_S3_BUCKET_NAMEE = "predictionBucket";
    private static final String TEST_TGT_S3_FOLDER = "resources/tgt";
    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    CreateForecastExportJobHandler handler;

    @BeforeEach
    public void setup() {
        // Setup Env variables
        environmentVariables.set("FORECAST_EXPORT_RESULT_ROLE_ARN", TEST_FORECAST_EXPORT_RESULT_ROLE_ARN);
        environmentVariables.set("PREDICTION_S3_BUCKET_NAME", TEST_PREDICTION_S3_BUCKET_NAMEE);
        environmentVariables.set("TGT_S3_FOLDER", TEST_TGT_S3_FOLDER);

        handler = new CreateForecastExportJobHandler(mockForecastClient);
    }

    @Test
    public void testProcess_withActiveStatus() {
        DescribeForecastExportJobResult dummyDescribeForecastExportJobResult = new DescribeForecastExportJobResult().withStatus(RESOURCE_ACTIVE_STATUS);
        when(mockForecastClient.describeForecastExportJob(any(DescribeForecastExportJobRequest.class))).thenReturn(dummyDescribeForecastExportJobResult);

        handler.process(buildResourceIdMap(System.currentTimeMillis(), TEST_FORECAST_RESOURCE_ARN, DEFAULT_DATA_FREQUENCY_VALUE));

        verify(mockForecastClient, times(1)).describeForecastExportJob(any(DescribeForecastExportJobRequest.class));
        verify(mockForecastClient, never()).createForecastExportJob(any(CreateForecastExportJobRequest.class));
    }

    @Test
    public void testProcess_withFailedStatus() {
        DescribeForecastExportJobResult dummyDescribeForecastExportJobResult = new DescribeForecastExportJobResult().withStatus(RESOURCE_FAILED_STATUS);
        when(mockForecastClient.describeForecastExportJob(any(DescribeForecastExportJobRequest.class))).thenReturn(dummyDescribeForecastExportJobResult);

        assertThrows(ResourceSetupFailureException.class,
                () -> handler.process(buildResourceIdMap(System.currentTimeMillis(), TEST_FORECAST_RESOURCE_ARN, DEFAULT_DATA_FREQUENCY_VALUE)));

        verify(mockForecastClient, times(1)).describeForecastExportJob(any(DescribeForecastExportJobRequest.class));
        verify(mockForecastClient, never()).createForecastExportJob(any(CreateForecastExportJobRequest.class));
    }

    @Test
    public void testProcess_withCreateNewForecastExportJobThenInCreating() {
        DescribeForecastExportJobResult dummyDescribeForecastExportJobResult = new DescribeForecastExportJobResult().withStatus(TEST_RESOURCE_CREATING_STATUS);
        when(mockForecastClient.describeForecastExportJob(any(DescribeForecastExportJobRequest.class)))
                .thenAnswer(new Answer<DescribeForecastExportJobResult>() {
                    private int count = 0;

                    @Override
                    public DescribeForecastExportJobResult answer(InvocationOnMock invocation) {
                        switch (count++) {
                            case 0:
                                throw new ResourceNotFoundException("cannot find given forecast export job");
                            case 1:
                                return dummyDescribeForecastExportJobResult;
                            default:
                                throw new IllegalArgumentException();
                        }
                    }
                });

        assertThrows(ResourceSetupInProgressException.class,
                () -> handler.process(buildResourceIdMap(System.currentTimeMillis(), TEST_FORECAST_RESOURCE_ARN, DEFAULT_DATA_FREQUENCY_VALUE)));

        verify(mockForecastClient, times(2)).describeForecastExportJob(any(DescribeForecastExportJobRequest.class));
        verify(mockForecastClient, times(1)).createForecastExportJob(any(CreateForecastExportJobRequest.class));
    }

    @Test
    public void testProcess_withCreateNewForecastExportJobThenInActive() {
        DescribeForecastExportJobResult dummyDescribeForecastExportJobResult = new DescribeForecastExportJobResult().withStatus(RESOURCE_ACTIVE_STATUS);
        when(mockForecastClient.describeForecastExportJob(any(DescribeForecastExportJobRequest.class)))
                .thenAnswer(new Answer<DescribeForecastExportJobResult>() {
                    private int count = 0;

                    @Override
                    public DescribeForecastExportJobResult answer(InvocationOnMock invocation) {
                        switch (count++) {
                            case 0:
                                throw new ResourceNotFoundException("cannot find given forecast export job");
                            case 1:
                                return dummyDescribeForecastExportJobResult;
                            default:
                                throw new IllegalArgumentException();
                        }
                    }
                });

        handler.process(buildResourceIdMap(System.currentTimeMillis(), TEST_FORECAST_RESOURCE_ARN, DEFAULT_DATA_FREQUENCY_VALUE));

        verify(mockForecastClient, times(2)).describeForecastExportJob(any(DescribeForecastExportJobRequest.class));
        verify(mockForecastClient, times(1)).createForecastExportJob(any(CreateForecastExportJobRequest.class));
    }
}
