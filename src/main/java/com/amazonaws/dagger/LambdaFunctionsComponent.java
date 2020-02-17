package com.amazonaws.dagger;

import com.amazonaws.lambda.demandpublishing.PublishDemandHandler;
import com.amazonaws.lambda.predictiongeneration.AbstractPredictionGenerationLambdaHandler;
import com.amazonaws.lambda.predictiongeneration.GenerateForecastResourcesIdsCronHandler;
import com.amazonaws.lambda.predictiongeneration.GenerateForecastResourcesIdsHandler;
import com.amazonaws.lambda.queryingpredictionresult.LoadDataFromS3ToDynamoDBHandler;
import dagger.Component;

import javax.inject.Singleton;

@Singleton
@Component(modules = {AWSClientModule.class})
public interface LambdaFunctionsComponent {

    void inject(PublishDemandHandler handler);

    void inject(AbstractPredictionGenerationLambdaHandler handler);

    void inject(GenerateForecastResourcesIdsHandler handler);

    void inject(GenerateForecastResourcesIdsCronHandler handler);

    void inject(LoadDataFromS3ToDynamoDBHandler handler);
}
