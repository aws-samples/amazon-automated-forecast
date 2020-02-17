package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.lambda.predictiongeneration.exception.ResourceSetupInProgressException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;

import static com.amazonaws.lambda.predictiongeneration.GenerateForecastResourcesIdsHandler.buildResourceIdMap;
import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.ONE_HOUR_DATA_FREQUENCY_STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GenerateForecastResourcesIdsHandlerTest extends BaseTest {

    private Clock fixedClock;
    private AmazonS3 mockS3Client;
    private GenerateForecastResourcesIdsHandler handler;

    @BeforeEach
    void setup() {
        fixedClock = Clock.fixed(LocalDateTime.of(2019, 1, 1, 1, 1)
                .toInstant(ZoneOffset.UTC), ZoneId.of("UTC"));
        mockS3Client = mock(AmazonS3.class);
        handler = new GenerateForecastResourcesIdsHandler(fixedClock, mockS3Client);
    }

    @Test
    public void testHandleRequest_WithNoSourceFileExist() {

        when(mockS3Client.getObjectMetadata(any(GetObjectMetadataRequest.class)))
                .thenThrow(AmazonS3Exception.class);

        assertThrows(ResourceSetupInProgressException.class,
                () -> handler.handleRequest(null, mock(Context.class)));
    }

    @Test
    public void testHandleRequest() throws Exception {

        long currentTime = fixedClock.millis();
        Map<String, String> expectedResourceIdMap = buildResourceIdMap(currentTime, TEST_FORECAST_RESOURCE_ARN,
                ONE_HOUR_DATA_FREQUENCY_STRING);

        Context mockContext = mock(Context.class);
        when(mockContext.getInvokedFunctionArn()).thenReturn(TEST_FUNCTION_ARN);
        ObjectMetadata dummyObjectMetadata = new ObjectMetadata();
        dummyObjectMetadata.setContentLength(100);
        when(mockS3Client.getObjectMetadata(any(GetObjectMetadataRequest.class))).thenReturn(dummyObjectMetadata);

        String mapString = handler.handleRequest(null, mockContext);
        Map<String, String> actualResourceIdMap = new ObjectMapper().readValue(mapString, new TypeReference<Map<String, String>>() {
        });
        assertEquals(expectedResourceIdMap, actualResourceIdMap);

    }
}
