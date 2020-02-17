package com.amazonaws.lambda.predictiongeneration;


import com.amazonaws.services.forecast.model.DatasetImportJobSummary;
import com.amazonaws.services.forecast.model.DatasetSummary;
import com.amazonaws.services.forecast.model.DeleteDatasetImportJobRequest;
import com.amazonaws.services.forecast.model.Filter;
import com.amazonaws.services.forecast.model.FilterConditionString;
import com.amazonaws.services.forecast.model.ListDatasetImportJobsRequest;
import com.amazonaws.services.forecast.model.ListDatasetImportJobsResult;
import com.amazonaws.services.forecast.model.ListDatasetsRequest;
import com.amazonaws.services.forecast.model.ListDatasetsResult;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;

import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.DATASET_ARN_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeleteOutdatedDatasetImportJobsHandlerTest extends BaseTest {

    DeleteOutdatedDatasetImportJobsHandler handler;

    @BeforeEach
    public void setup() {
        handler = new DeleteOutdatedDatasetImportJobsHandler(mockForecastClient);
    }

    @Test
    public void testProcess_withNoOutdatedDatasets() {
        ListDatasetsResult dummyListDatasetsResult = new ListDatasetsResult()
                .withDatasets(Collections.singletonList(new DatasetSummary().withDatasetArn(testResourceIdMap.get(DATASET_ARN_KEY))));
        when(mockForecastClient.listDatasets(any(ListDatasetsRequest.class))).thenReturn(dummyListDatasetsResult);

        handler.process(testResourceIdMap);

        verify(mockForecastClient, times(1)).listDatasets(any(ListDatasetsRequest.class));
        verify(mockForecastClient, never()).listDatasetImportJobs(any(ListDatasetImportJobsRequest.class));
        verify(mockForecastClient, never()).deleteDatasetImportJob(any(DeleteDatasetImportJobRequest.class));
    }

    @Test
    public void testProcess_withNoOutdatedDatasetImportJobs() {
        String dummyOutdatedDatasetArn = "dummyOutdatedDatasetArn";
        ListDatasetsResult dummyListDatasetsResult = new ListDatasetsResult()
                .withDatasets(Lists.newArrayList(new DatasetSummary().withDatasetArn(testResourceIdMap.get(DATASET_ARN_KEY)),
                        new DatasetSummary().withDatasetArn(dummyOutdatedDatasetArn)));
        when(mockForecastClient.listDatasets(any(ListDatasetsRequest.class))).thenReturn(dummyListDatasetsResult);
        when(mockForecastClient.listDatasetImportJobs(eq(new ListDatasetImportJobsRequest()
                .withFilters(
                        new Filter()
                                .withKey("DatasetArn")
                                .withValue(dummyOutdatedDatasetArn)
                                .withCondition(FilterConditionString.IS)))))
                .thenReturn(new ListDatasetImportJobsResult().withDatasetImportJobs());

        handler.process(testResourceIdMap);

        verify(mockForecastClient, times(1)).listDatasets(any(ListDatasetsRequest.class));
        verify(mockForecastClient, times(1)).listDatasetImportJobs(any(ListDatasetImportJobsRequest.class));
        verify(mockForecastClient, never()).deleteDatasetImportJob(any(DeleteDatasetImportJobRequest.class));
    }

    @Test
    public void testProcess_withAbleToDeleteDatasetImportJobs() {
        when(mockForecastClient.listDatasets(any(ListDatasetsRequest.class))).thenReturn(new ListDatasetsResult().withDatasets(
                Lists.newArrayList(new DatasetSummary().withDatasetArn("dummyOutdatedDatasetArn"),
                        new DatasetSummary().withDatasetArn(testResourceIdMap.get(DATASET_ARN_KEY)))
        ));

        when(mockForecastClient.listDatasetImportJobs(eq(new ListDatasetImportJobsRequest()
                .withFilters(
                        new Filter()
                                .withKey("DatasetArn")
                                .withValue("dummyOutdatedDatasetArn")
                                .withCondition(FilterConditionString.IS)
                ))))
                .thenAnswer(new Answer<ListDatasetImportJobsResult>() {

                    private int count = 0;

                    @Override
                    public ListDatasetImportJobsResult answer(InvocationOnMock invocation) {
                        switch (count++) {
                            case 0:
                                return new ListDatasetImportJobsResult()
                                        .withDatasetImportJobs(new DatasetImportJobSummary().withDatasetImportJobArn("dummyDijArn"));
                            case 1:
                                return new ListDatasetImportJobsResult().withDatasetImportJobs();
                            default:
                                throw new IllegalArgumentException();
                        }
                    }
                });

        handler.process(testResourceIdMap);

        verify(mockForecastClient, times(2)).listDatasets(any(ListDatasetsRequest.class));
        verify(mockForecastClient, times(2)).listDatasetImportJobs(any(ListDatasetImportJobsRequest.class));
        verify(mockForecastClient, times(1)).deleteDatasetImportJob(any(DeleteDatasetImportJobRequest.class));
    }
}
