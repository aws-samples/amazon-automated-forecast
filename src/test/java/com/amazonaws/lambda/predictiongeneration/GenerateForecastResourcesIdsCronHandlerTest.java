package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.services.forecast.model.DatasetSummary;
import com.amazonaws.services.forecast.model.ListDatasetsRequest;
import com.amazonaws.services.forecast.model.ListDatasetsResult;
import com.amazonaws.services.forecast.model.ListPredictorsRequest;
import com.amazonaws.services.forecast.model.ListPredictorsResult;
import com.amazonaws.services.forecast.model.PredictorSummary;
import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Map;

import static com.amazonaws.lambda.predictiongeneration.GenerateForecastResourcesIdsCronHandler.buildCronResourceIdMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GenerateForecastResourcesIdsCronHandlerTest extends BaseTest {

    private Clock fixedClock;
    private GenerateForecastResourcesIdsCronHandler handler;

    @BeforeEach
    public void setup() {
        fixedClock = Clock.fixed(LocalDateTime.of(2019, 1, 1, 1, 1)
                .toInstant(ZoneOffset.UTC), ZoneId.of("UTC"));
        handler = new GenerateForecastResourcesIdsCronHandler(fixedClock, mockForecastClient);
    }

    @Test
    public void testHandleRequest_WithNoDataset() {
        when(mockForecastClient.listDatasets(any(ListDatasetsRequest.class))).thenReturn(new ListDatasetsResult().withDatasets());
        assertThrows(IllegalStateException.class, () -> handler.handleRequest(null, mock(Context.class)));
    }

    @Test
    public void testHandleRequest() throws Exception {
        long currentTime = fixedClock.millis();

        Context mockContext = mock(Context.class);
        when(mockContext.getInvokedFunctionArn()).thenReturn(TEST_FUNCTION_ARN);
        String dummyDatasetName = "dummyDatasetName";
        when(mockForecastClient.listDatasets(any(ListDatasetsRequest.class)))
                .thenReturn(new ListDatasetsResult()
                        .withDatasets(new DatasetSummary().withDatasetName(dummyDatasetName).withCreationTime(new Date())));
        String dummyPredictorArn = "dummyPredictorArn";
        when(mockForecastClient.listPredictors(any(ListPredictorsRequest.class)))
                .thenReturn(new ListPredictorsResult()
                        .withPredictors(new PredictorSummary().withPredictorArn(dummyPredictorArn).withCreationTime(new Date())));
        Map<String, String> expectedResourceIdMap = buildCronResourceIdMap(currentTime,
                TEST_FORECAST_RESOURCE_ARN, dummyDatasetName, dummyPredictorArn);

        String mapString = handler.handleRequest(null, mockContext);
        Map<String, String> actualResourceIdMap = new ObjectMapper().readValue(mapString, new TypeReference<Map<String, String>>() {
        });
        assertEquals(expectedResourceIdMap, actualResourceIdMap);
    }
}
