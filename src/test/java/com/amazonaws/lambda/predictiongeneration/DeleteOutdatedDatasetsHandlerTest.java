package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.lambda.predictiongeneration.exception.ResourceCleanupInProgressException;
import com.amazonaws.services.forecast.model.DatasetSummary;
import com.amazonaws.services.forecast.model.DeleteDatasetRequest;
import com.amazonaws.services.forecast.model.ListDatasetsRequest;
import com.amazonaws.services.forecast.model.ListDatasetsResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_ARN_KEY;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeleteOutdatedDatasetsHandlerTest extends BaseTest {

    DeleteOutdatedDatasetsHandler handler;

    @BeforeEach
    public void setup() {
        handler = new DeleteOutdatedDatasetsHandler(mockForecastClient);
    }

    @Test
    public void testProcess_withNoOutdatedDataset() {
        ListDatasetsResult dummyListDatasetsResult = new ListDatasetsResult()
                .withDatasets(Collections.singletonList(new DatasetSummary().withDatasetArn(testResourceIdMap.get(DATASET_ARN_KEY))));
        when(mockForecastClient.listDatasets(any(ListDatasetsRequest.class))).thenReturn(dummyListDatasetsResult);

        handler.process(testResourceIdMap);

        verify(mockForecastClient, times(1)).listDatasets(any(ListDatasetsRequest.class));
        verify(mockForecastClient, never()).deleteDataset(any(DeleteDatasetRequest.class));
    }

    @Test
    public void testProcess_withUnableToDeleteDatasets() {
        List<DatasetSummary> dummyOutdatedDatasets = new ArrayList<>();
        dummyOutdatedDatasets.add(new DatasetSummary().withDatasetArn("dummy1"));
        dummyOutdatedDatasets.add(new DatasetSummary().withDatasetArn("dummy2"));
        List<DatasetSummary> dummyExistingDatasets = new ArrayList<>();
        dummyExistingDatasets.add(new DatasetSummary().withDatasetArn(testResourceIdMap.get(DATASET_ARN_KEY)));
        dummyExistingDatasets.addAll(dummyOutdatedDatasets);
        ListDatasetsResult dummyListDatasetsResult = new ListDatasetsResult().withDatasets(dummyExistingDatasets);
        when(mockForecastClient.listDatasets(any(ListDatasetsRequest.class))).thenReturn(dummyListDatasetsResult);

        assertThrows(ResourceCleanupInProgressException.class, () -> handler.process(testResourceIdMap));

        verify(mockForecastClient, times(2)).listDatasets(any(ListDatasetsRequest.class));
        verify(mockForecastClient, times(dummyOutdatedDatasets.size())).deleteDataset(any(DeleteDatasetRequest.class));
    }

    @Test
    public void testProcess_withAbleToDeleteDatasets() {
        DatasetSummary testPreservedDataset = new DatasetSummary().withDatasetArn(testResourceIdMap.get(DATASET_ARN_KEY));
        List<DatasetSummary> dummyOutdatedDatasets= new ArrayList<>();
        dummyOutdatedDatasets.add(new DatasetSummary().withDatasetArn("dummy1"));
        dummyOutdatedDatasets.add(new DatasetSummary().withDatasetArn("dummy2"));
        when(mockForecastClient.listDatasets(any(ListDatasetsRequest.class)))
                .thenAnswer(new Answer<ListDatasetsResult>() {
                    private int count = 0;

                    @Override
                    public ListDatasetsResult answer(InvocationOnMock invocation) {
                        switch (count++) {
                            case 0:
                                List<DatasetSummary> withOutdatedDatasets = new ArrayList<>();
                                withOutdatedDatasets.add(testPreservedDataset);
                                withOutdatedDatasets.addAll(dummyOutdatedDatasets);
                                ListDatasetsResult withOutdatedResult = new ListDatasetsResult().withDatasets(withOutdatedDatasets);
                                return withOutdatedResult;
                            case 1:
                                ListDatasetsResult withoutOutdatedResponse = new ListDatasetsResult().withDatasets(Collections.singletonList(testPreservedDataset));
                                return withoutOutdatedResponse;
                            default:
                                throw new IllegalArgumentException();
                        }
                    }
                });

        handler.process(testResourceIdMap);

        verify(mockForecastClient, times(2)).listDatasets(any(ListDatasetsRequest.class));
        verify(mockForecastClient, times(dummyOutdatedDatasets.size())).deleteDataset(any(DeleteDatasetRequest.class));
    }
}
