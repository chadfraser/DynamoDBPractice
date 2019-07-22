package com.fraser.amazontutorial;

import java.util.HashMap;
import java.util.Iterator;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;

public class QueryTest {
    public static void main(String[] args) {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000",
                        "us-west-2"))
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable("movies");
        queryMoviesFrom1985(table);
        queryMoviesWithTitleInRangeAToL(table);
    }

    private static void queryMoviesFrom1985(Table table) {
        HashMap<String, String> nameMap = new HashMap<String, String>();
        nameMap.put("#yr", "year");

        HashMap<String, Object> valueMap = new HashMap<String, Object>();
        valueMap.put(":yyyy", 1985);

        QuerySpec querySpec = new QuerySpec().withKeyConditionExpression("#yr = :yyyy")
                .withNameMap(nameMap).withValueMap(valueMap);

        ItemCollection<QueryOutcome> itemCollection;
        Iterator<Item> itemIterator;
        Item item;

        try {
            System.out.println("Movies from 1985:");
            itemCollection = table.query(querySpec);
            itemIterator = itemCollection.iterator();

            while (itemIterator.hasNext()) {
                item = itemIterator.next();
                System.out.println(String.format("\t%d: %s", item.getNumber("year").intValue(),
                        item.getString("title")));
            }
        } catch (Exception e) {
            System.err.println("Unable to query movies from 1985.");
            System.err.println(e.getMessage());
        }
    }

    private static void queryMoviesWithTitleInRangeAToL(Table table) {
        HashMap<String, String> nameMap = new HashMap<String, String>();
        nameMap.put("#yr", "year");

        HashMap<String, Object> valueMap = new HashMap<String, Object>();
        valueMap.put(":yyyy", 1992);

        QuerySpec querySpec = new QuerySpec().withProjectionExpression("#yr, title, info.genres, info.actors[0]")
                .withKeyConditionExpression("#yr = :yyyy")
                .withNameMap(nameMap).withValueMap(valueMap);

        ItemCollection<QueryOutcome> itemCollection;
        Iterator<Item> itemIterator;
        Item item;

        try {
            System.out.println("Movies from 1992 with titles A-L, with genre and lead actor:");
            itemCollection = table.query(querySpec);
            itemIterator = itemCollection.iterator();

            while (itemIterator.hasNext()) {
                item = itemIterator.next();
                System.out.println(String.format("\t%d: %s %s", item.getNumber("year").intValue(),
                        item.getString("title"), item.getMap("info")));
            }
        } catch (Exception e) {
            System.err.println("Unable to query movies from 1985.");
            System.err.println(e.getMessage());
        }
    }
}
