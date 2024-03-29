package com.fraser.amazontutorial;

import java.io.File;
import java.util.Iterator;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class MovieLoadDataTest {
    public static void main(String[] args) throws Exception {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000",
                        "us-west-2"))
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable("movies");

        JsonParser parser = new JsonFactory().createParser(new File("res/moviedata.json"));
        JsonNode rootNode = new ObjectMapper().readTree(parser);
        Iterator<JsonNode> jsonNodeIterator = rootNode.iterator();

        ObjectNode currentNode;

        while (jsonNodeIterator.hasNext()) {
            currentNode = (ObjectNode) jsonNodeIterator.next();
            int year = currentNode.path("year").asInt();
            String title = currentNode.path("title").asText();

            try {
                table.putItem(new Item().withPrimaryKey("year", year, "title", title)
                        .withJSON("info", currentNode.path("info").toString()));
                System.out.println(String.format("PutItem success! %d %s", year, title));
            } catch (Exception e) {
                System.err.println(String.format("Unable to add movie: (%d, %s)", year, title));
                System.err.println(e.getMessage());
                break;
            }
        }
        parser.close();
    }
}
