package com.amazonaws.lambda.predictiongeneration;

import com.amazonaws.lambda.predictiongeneration.exception.ResourceCleanupInProgressException;
import com.amazonaws.services.forecast.model.DeletePredictorRequest;
import com.amazonaws.services.forecast.model.ListPredictorsRequest;
import com.amazonaws.services.forecast.model.ListPredictorsResult;
import com.amazonaws.services.forecast.model.PredictorSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.amazonaws.lambda.predictiongeneration.PredictionGenerationUtils.PREDICTOR_ARN_KEY;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeleteOutdatedPredictorsHandlerTest extends BaseTest {

    DeleteOutdatedPredictorsHandler handler;

    @BeforeEach
    public void setup() {
        handler = new DeleteOutdatedPredictorsHandler(mockForecastClient);
    }

    @Test
    public void testProcess_withNoOutdatedPredictor() {
        ListPredictorsResult dummyListPredictorsResult = new ListPredictorsResult()
                .withPredictors(Collections.singletonList(new PredictorSummary().withPredictorArn(testResourceIdMap.get(PREDICTOR_ARN_KEY))));
        when(mockForecastClient.listPredictors(any(ListPredictorsRequest.class))).thenReturn(dummyListPredictorsResult);

        handler.process(testResourceIdMap);

        verify(mockForecastClient, times(1)).listPredictors(any(ListPredictorsRequest.class));
        verify(mockForecastClient, never()).deletePredictor(any(DeletePredictorRequest.class));
    }

    @Test
    public void testProcess_withUnableToDeletePredictor() {
        List<PredictorSummary> dummyOutdatedPredictors = new ArrayList<>();
        dummyOutdatedPredictors.add(new PredictorSummary().withPredictorArn("dummy1"));
        dummyOutdatedPredictors.add(new PredictorSummary().withPredictorArn("dummy1"));
        List<PredictorSummary> dummyExistingPredictors = new ArrayList<>();
        dummyExistingPredictors.add(new PredictorSummary().withPredictorArn(testResourceIdMap.get(PREDICTOR_ARN_KEY)));
        dummyExistingPredictors.addAll(dummyOutdatedPredictors);
        ListPredictorsResult dummyListPredictorsResult = new ListPredictorsResult().withPredictors(dummyExistingPredictors);
        when(mockForecastClient.listPredictors(any(ListPredictorsRequest.class))).thenReturn(dummyListPredictorsResult);

        assertThrows(ResourceCleanupInProgressException.class, () -> handler.process(testResourceIdMap));

        verify(mockForecastClient, times(2)).listPredictors(any(ListPredictorsRequest.class));
        verify(mockForecastClient, times(dummyOutdatedPredictors.size())).deletePredictor(any(DeletePredictorRequest.class));
    }

    @Test
    public void testProcess_withAbleToDeletePredictors() {
        PredictorSummary testPreservedPredictor = new PredictorSummary().withPredictorArn(testResourceIdMap.get(PREDICTOR_ARN_KEY));

        List<PredictorSummary> dummyOutdatedPredictors = new ArrayList<>();
        dummyOutdatedPredictors.add(new PredictorSummary().withPredictorArn("dummy1"));
        dummyOutdatedPredictors.add(new PredictorSummary().withPredictorArn("dummy2"));


        when(mockForecastClient.listPredictors(any(ListPredictorsRequest.class)))
                .thenAnswer(new Answer<ListPredictorsResult>() {
                    private int count = 0;

                    @Override
                    public ListPredictorsResult answer(InvocationOnMock invocation) {
                        switch (count++) {
                            case 0:
                                List<PredictorSummary> withOutdatedPredictors = new ArrayList<>();
                                withOutdatedPredictors.add(testPreservedPredictor);
                                withOutdatedPredictors.addAll(dummyOutdatedPredictors);
                                ListPredictorsResult withOutdatedResult = new ListPredictorsResult().withPredictors(withOutdatedPredictors);
                                return withOutdatedResult;
                            case 1:
                                ListPredictorsResult withoutOutdatedResult = new ListPredictorsResult()
                                        .withPredictors(Collections.singletonList(testPreservedPredictor));
                                return withoutOutdatedResult;
                            default:
                                throw new IllegalArgumentException();
                        }
                    }
                });

        handler.process(testResourceIdMap);

        verify(mockForecastClient, times(2)).listPredictors(any(ListPredictorsRequest.class));
        verify(mockForecastClient, times(dummyOutdatedPredictors.size())).deletePredictor(any(DeletePredictorRequest.class));
    }
}
