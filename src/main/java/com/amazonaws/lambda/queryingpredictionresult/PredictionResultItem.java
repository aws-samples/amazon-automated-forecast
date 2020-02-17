package com.amazonaws.lambda.queryingpredictionresult;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.opencsv.bean.CsvBindByName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@DynamoDBTable(tableName = PredictionResultItem.TABLE_NAME)
public class PredictionResultItem {

    public static final String TABLE_NAME = "PredictionResultItem";

    public static class Attribute {
        public static final String ITEM_ID          = "item_id";
        public static final String DATE             = "date";
        public static final String P10              = "p10";
        public static final String P50              = "p50";
        public static final String P90              = "p90";
        public static final String EXPIRATION_TIME  = "expirationTime";
    }

    @DynamoDBHashKey(attributeName = Attribute.ITEM_ID)
    @CsvBindByName(column = Attribute.ITEM_ID, required = true)
    private String hashKey;

    @DynamoDBRangeKey(attributeName = Attribute.DATE)
    @CsvBindByName(column = Attribute.DATE, required = true)
    private String sortKey;

    @DynamoDBAttribute(attributeName = Attribute.P10)
    @CsvBindByName(column = Attribute.P10, required = true)
    private double p10;

    @DynamoDBAttribute(attributeName = Attribute.P50)
    @CsvBindByName(column = Attribute.P50, required = true)
    private double p50;

    @DynamoDBAttribute(attributeName = Attribute.P90)
    @CsvBindByName(column = Attribute.P90, required = true)
    private double p90;

    @DynamoDBAttribute(attributeName = Attribute.EXPIRATION_TIME)
    @CsvBindByName
    private long expirationTime;
}

