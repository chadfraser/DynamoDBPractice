package com.fraser.amazontutorial.testops;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;

public class UpdateConditionalTest {
    public static void main(String[] args) {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000",
                        "us-west-2"))
                .build();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable("movies");

        int year = 2015;
        String title = "The Big New Movie";

        UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey(new PrimaryKey("year", year,
                    "title", title))
                .withUpdateExpression("remove info.actors[0]")
                .withConditionExpression("size(info.actors) >= :num")
                .withValueMap(new ValueMap().withNumber(":num", 3))
                .withReturnValues(ReturnValue.UPDATED_NEW);

        try {
            System.out.println("Updating the item conditionally...");
            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
            System.out.println(String.format("PutItem success! %s", outcome.getItem().toJSONPretty()));
        } catch (Exception e) {
            // Note that this triggers an illegal reflective access warning. Look into why
            System.err.println(String.format("Unable to update item: (%d %s)", year, title));
            System.err.println(e.getMessage());
        }
    }
}
