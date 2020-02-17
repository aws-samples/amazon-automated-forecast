package com.amazonaws.dagger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.forecast.AmazonForecast;
import com.amazonaws.services.forecast.AmazonForecastClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module
public class AWSClientModule {

    private static final int NUMBER_OF_RETRIES = 10;
    private static final RetryPolicy RETRY_POLICY = new RetryPolicy(PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
            PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY, NUMBER_OF_RETRIES, false);
    private static final ClientConfiguration CLIENT_CONFIG = new ClientConfiguration().withRetryPolicy(RETRY_POLICY);

    @Provides
    @Singleton
    static AmazonDynamoDB provideDDBClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withClientConfiguration(CLIENT_CONFIG)
                .withRegion(Regions.fromName(System.getenv("AWS_REGION")))
                .build();
    }

    @Provides
    @Singleton
    static AmazonForecast provideForecastClient() {
        return AmazonForecastClientBuilder.standard()
                .withClientConfiguration(CLIENT_CONFIG)
                .withRegion(Regions.fromName(System.getenv("AWS_REGION")))
                .build();
    }

    @Provides
    @Singleton
    static AmazonS3 provideS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withClientConfiguration(CLIENT_CONFIG)
                .withRegion(Regions.fromName(System.getenv("AWS_REGION")))
                .build();
    }
}