package com.fraser.amazontutorial.testops;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;

public class DeleteTest {
    public static void main(String[] args) {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000",
                        "us-west-2"))
                .build();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable("movies");

        int year = 2015;
        String title = "The Big New Movie";

        DeleteItemSpec deleteItemSpec = new DeleteItemSpec().withPrimaryKey(new PrimaryKey("year", year,
                "title", title));
//                .withConditionExpression("info.rating <= :val")
//                .withValueMap(new ValueMap().withNumber(":val", 7));

        try {
            System.out.println("Deleting the item conditionally...");
            table.deleteItem(deleteItemSpec);
            System.out.println("DeleteItem success!");
        } catch (Exception e) {
            System.err.println(String.format("Unable to delete item: (%d %s)", year, title));
            System.err.println(e.getMessage());
        }
    }
}
