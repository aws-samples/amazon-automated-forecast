package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.lambda.predictiongeneration.exception.ResourceCleanupInProgressException;
import com.amazonaws.services.forecast.model.DatasetGroupSummary;
import com.amazonaws.services.forecast.model.DeleteDatasetGroupRequest;
import com.amazonaws.services.forecast.model.ListDatasetGroupsRequest;
import com.amazonaws.services.forecast.model.ListDatasetGroupsResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_GROUP_ARN_KEY;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeleteOutdatedDatasetGroupsHandlerTest extends BaseTest {

    DeleteOutdatedDatasetGroupsHandler handler;

    @BeforeEach
    public void setup() {
        handler = new DeleteOutdatedDatasetGroupsHandler(mockForecastClient);
    }

    @Test
    public void testProcess_withNoDatasetGroup() {
        ListDatasetGroupsResult dummyListDatasetGroupsResult = new ListDatasetGroupsResult().withDatasetGroups(Collections.emptyList());
        when(mockForecastClient.listDatasetGroups(any(ListDatasetGroupsRequest.class))).thenReturn(dummyListDatasetGroupsResult);

        assertThrows(IllegalStateException.class,
                () -> handler.process(testResourceIdMap));

        verify(mockForecastClient, times(1)).listDatasetGroups(any(ListDatasetGroupsRequest.class));
        verify(mockForecastClient, never()).deleteDatasetGroup(any(DeleteDatasetGroupRequest.class));
    }

    @Test
    public void testProcess_withNoOutdatedDatasetGroup() {
        ListDatasetGroupsResult dummyListDatasetGroupsResult = new ListDatasetGroupsResult()
                .withDatasetGroups(Collections.singleton(new DatasetGroupSummary().withDatasetGroupArn(testResourceIdMap.get(DATASET_GROUP_ARN_KEY))));
        when(mockForecastClient.listDatasetGroups(any(ListDatasetGroupsRequest.class))).thenReturn(dummyListDatasetGroupsResult);

        handler.process(testResourceIdMap);

        verify(mockForecastClient, times(1)).listDatasetGroups(any(ListDatasetGroupsRequest.class));
        verify(mockForecastClient, never()).deleteDatasetGroup(any(DeleteDatasetGroupRequest.class));
    }

    @Test
    public void testProcess_withUnableToDeleteDatasetGroups() {
        List<DatasetGroupSummary> dummyOutdatedDatasetGroups = new ArrayList<>();
        dummyOutdatedDatasetGroups.add(new DatasetGroupSummary().withDatasetGroupArn("dummy1"));
        dummyOutdatedDatasetGroups.add(new DatasetGroupSummary().withDatasetGroupArn("dummy2"));
        List<DatasetGroupSummary> dummyExistingDatasetGroups = new ArrayList<>();
        dummyExistingDatasetGroups.add(new DatasetGroupSummary().withDatasetGroupArn(testResourceIdMap.get(DATASET_GROUP_ARN_KEY)));
        dummyExistingDatasetGroups.addAll(dummyOutdatedDatasetGroups);
        ListDatasetGroupsResult dummyListDatasetGroupsResult = new ListDatasetGroupsResult().withDatasetGroups(dummyExistingDatasetGroups);
        when(mockForecastClient.listDatasetGroups(any(ListDatasetGroupsRequest.class))).thenReturn(dummyListDatasetGroupsResult);

        assertThrows(ResourceCleanupInProgressException.class,
                () -> handler.process(testResourceIdMap));

        verify(mockForecastClient, times(2)).listDatasetGroups(any(ListDatasetGroupsRequest.class));
        verify(mockForecastClient, times(dummyOutdatedDatasetGroups.size())).deleteDatasetGroup(any(DeleteDatasetGroupRequest.class));
    }

    @Test
    public void testProcess_withAbleToDeleteDatasetGroups() {
        DatasetGroupSummary testPreservedDatasetGroup = new DatasetGroupSummary()
                .withDatasetGroupArn(testResourceIdMap.get(DATASET_GROUP_ARN_KEY));
        List<DatasetGroupSummary> dummyOutdatedDatasetGroups= new ArrayList<>();
        dummyOutdatedDatasetGroups.add(new DatasetGroupSummary().withDatasetGroupArn("dummy1"));
        dummyOutdatedDatasetGroups.add(new DatasetGroupSummary().withDatasetGroupArn("dummy2"));
        when(mockForecastClient.listDatasetGroups(any(ListDatasetGroupsRequest.class)))
                .thenAnswer(new Answer<ListDatasetGroupsResult>() {
                    private int count = 0;

                    @Override
                    public ListDatasetGroupsResult answer(InvocationOnMock invocation) {
                        switch (count++) {
                            case 0:
                                List<DatasetGroupSummary> withOutdatedDatasets = new ArrayList<>();
                                withOutdatedDatasets.add(testPreservedDatasetGroup);
                                withOutdatedDatasets.addAll(dummyOutdatedDatasetGroups);
                                ListDatasetGroupsResult withOutdatedResult = new ListDatasetGroupsResult().withDatasetGroups(withOutdatedDatasets);
                                return withOutdatedResult;
                            case 1:
                                ListDatasetGroupsResult withoutOutdatedResult = new ListDatasetGroupsResult()
                                        .withDatasetGroups(Collections.singletonList(testPreservedDatasetGroup));
                                return withoutOutdatedResult;
                            default:
                                throw new IllegalArgumentException();
                        }
                    }
                });

        handler.process(testResourceIdMap);

        verify(mockForecastClient, times(2)).listDatasetGroups(any(ListDatasetGroupsRequest.class));
        verify(mockForecastClient, times(dummyOutdatedDatasetGroups.size())).deleteDatasetGroup(any(DeleteDatasetGroupRequest.class));
    }
}
