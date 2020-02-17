package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.services.forecast.model.CreateDatasetGroupRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.amazonaws.lambda.predictiongeneration.GenerateForecastResourcesIdsHandler.buildResourceIdMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CreateDatasetGroupHandlerTest extends BaseTest {

    CreateDatasetGroupHandler handler;

    @BeforeEach
    public void setup() {
        handler = new CreateDatasetGroupHandler(mockForecastClient);
    }

    @Test
    public void testProcess() {
        handler.process(buildResourceIdMap(System.currentTimeMillis(), TEST_FORECAST_RESOURCE_ARN, DEFAULT_DATA_FREQUENCY_VALUE));
        verify(mockForecastClient, times(1)).createDatasetGroup(any(CreateDatasetGroupRequest.class));
    }
}
