package com.amazonaws.lambda.demandpublishing;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.util.IOUtils;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.io.InputStream;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PublishDemandHandlerTest {

    private static final String PREDICTION_S3_BUCKET_NAME = "testBucket";
    private static final String SRC_S3_FOLDER = "testSrc";
    private static final String S3_TRAINING_DATA_FILE_NAME = "testDemandFile";

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Mock
    private Context context;

    private Clock fixedClock;
    private String testRawDemandRequestsFilePath;
    private TransferManager mockTransferManager;
    private PublishDemandHandler handler;

    @BeforeEach
    void setup() {
        environmentVariables.set("PREDICTION_S3_BUCKET_NAME", PREDICTION_S3_BUCKET_NAME);
        environmentVariables.set("SRC_S3_FOLDER", SRC_S3_FOLDER);
        environmentVariables.set("S3_TRAINING_DATA_FILE_NAME", S3_TRAINING_DATA_FILE_NAME);

        fixedClock = Clock.fixed(LocalDateTime.of(2023, 3, 1, 1, 1)
                .toInstant(ZoneOffset.UTC), ZoneId.of("UTC"));
        testRawDemandRequestsFilePath = "/test_raw_demand_requests.csv";
        mockTransferManager = mock(TransferManager.class);
        handler = new PublishDemandHandler(fixedClock, testRawDemandRequestsFilePath, mockTransferManager);
    }

    @Test
    public void testPublishDemand() throws Exception {
        // Check the first two records in src/test/resources/test_raw_demand_requests.csv for such info
        String expectedDemandRecordsStr = "item_id,timestamp,target_value\n5,2020-01-01 03:50:33,14\n5,2020-02-01 03:53:14,14";
        ObjectMetadata expectedMetadata = new ObjectMetadata();
        expectedMetadata.setContentLength(expectedDemandRecordsStr.length());
        when(mockTransferManager.upload(any(String.class), any(String.class), any(InputStream.class), any(ObjectMetadata.class)))
                .thenReturn(mock(Upload.class));

        handler.handleRequest(null, context);

        ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);
        ArgumentCaptor<ObjectMetadata> objectMetadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
        verify(mockTransferManager, times(1)).upload(eq(PREDICTION_S3_BUCKET_NAME),
                eq(String.format("%s/%s", SRC_S3_FOLDER, S3_TRAINING_DATA_FILE_NAME)),
                streamCaptor.capture(), objectMetadataCaptor.capture());
        assertEquals(expectedDemandRecordsStr, IOUtils.toString(streamCaptor.getValue()));
        assertEquals(expectedMetadata.getContentLength(), objectMetadataCaptor.getValue().getContentLength());
    }
}
